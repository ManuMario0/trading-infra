#[macro_export]
macro_rules! make_ipc {
    // Internal helper for building the factory call with one type
    (@build $transport:ident, $kind:ident, $role:ident, $tx:ty, (), $name:expr, $addr:expr, $reg:expr, $waker:expr) => {
        <($crate::factory::$transport, $crate::factory::$kind, $crate::factory::$role) as $crate::factory::SocketFactory<$tx, ()>>::build($name, $addr, $reg, $waker)
    };

    // Internal helper for building the factory call with two types
    (@build $transport:ident, $kind:ident, $role:ident, $tx:ty, ($rx:ty), $name:expr, $addr:expr, $reg:expr, $waker:expr) => {
        <($crate::factory::$transport, $crate::factory::$kind, $crate::factory::$role) as $crate::factory::SocketFactory<$tx, $rx>>::build($name, $addr, $reg, $waker)
    };

    (
        $(
            $name:ident : kind=$kind:ident, role=$role:ident, ty=$msg_type:ty, $(rep_ty=$rep_msg_type:ty,)? addr=$addr:expr, transport=$transport:ident
        ),* $(,)?
    ) => {{
        use std::sync::Arc;
        use $crate::{Poll, Waker};
        use $crate::token::TokenRegistry;
        use $crate::runner::{IpcRunner, IpcHandle, WAKER_TOKEN};
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
                    let pair = $crate::make_ipc!(@build
                        $transport, $kind, $role, $msg_type,
                        ($($rep_msg_type)?),
                        stringify!($name),
                        $addr,
                        TokenRegistry::new(_prefix_counter, registry.clone()),
                        waker.clone()
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
            PORT_A: kind=Aggregate, role=Bind, ty=u64, addr="127.0.0.1:8001", transport=TCP,
            PORT_B: kind=Broadcast, role=Connect, ty=u64, addr="127.0.0.1:8002", transport=TCP,
        );

        assert_eq!(blueprint.ports.len(), 2);
    }
}
