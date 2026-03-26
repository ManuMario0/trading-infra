#[macro_export]
macro_rules! make_ipc {
    (
        $(
            $name:ident : kind=$kind:ident, role=$role:ident, capacity=$cap:expr, ty=$msg_type:ty, addr=$addr:expr, transport=$transport:ident
        ),* $(,)?
    ) => {{
        use std::sync::Arc;
        use $crate::{Poll, Waker};
        use $crate::token::TokenRegistry;
        use $crate::runner::{IpcRunner, IpcHandle, WAKER_TOKEN};
        use $crate::factory::SocketFactory;
        use $crate::blueprint::{Blueprint, PortDefinition};
        use $crate::socket::{SocketKind, SocketRole};

        let poll = Poll::new().expect("Failed to create Poll");
        let registry = Arc::new(poll.registry().try_clone().expect("Failed to clone registry"));
        let waker = Arc::new(Waker::new(&registry, WAKER_TOKEN).expect("Failed to create waker"));

        let handle = IpcHandle::new(waker.clone());

        // A temporary struct to flawlessly unzip the macro repetitions into
        // cleanly separated engine tuples and socket tuples without needing `paste!`
        #[allow(non_snake_case, non_camel_case_types)]
        struct __MacroPairs< $($name),* > {
            $( $name: $name ),*
        }

        let mut _prefix_counter = 0u8;

        let pairs = __MacroPairs {
            $(
                $name: {
                    let pair = <($crate::factory::$transport, $crate::factory::$kind, $crate::factory::$role) as SocketFactory<$msg_type>>::build(
                        stringify!($name),
                        $addr,
                        $cap,
                         TokenRegistry::new(_prefix_counter, registry.clone()),
                        waker.clone(),
                    );
                    _prefix_counter += 1;
                    pair
                }
            ),*
        };

        let runner = IpcRunner::new(poll, ( $( pairs.$name.0, )* ), waker.clone());

        let blueprint = Blueprint {
            ports: vec![ $(
                PortDefinition::new(
                    stringify!($name).to_string(),
                    stringify!($msg_type).to_string(),
                    SocketKind::$kind,
                    SocketRole::$role,
                    $cap
                )
            ),* ],
        };

        // Return (runner, handle, socket1, socket2, ...) exact order!
        ( runner, blueprint, handle, $( pairs.$name.1 ),* )
    }};
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_make_ipc() {
        let (_runner, blueprint, _handle, _socket1, _socket2) = make_ipc!(
            PORT_A: kind=Aggregate, role=Bind, capacity=1024, ty=u64, addr="127.0.0.1:8001", transport=TCP,
            PORT_B: kind=Broadcast, role=Connect, capacity=1024, ty=u64, addr="127.0.0.1:8002", transport=TCP,
        );

        assert_eq!(blueprint.ports.len(), 2);
    }
}
