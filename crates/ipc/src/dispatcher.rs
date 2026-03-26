use mio::Token;

use crate::token::{self, LocalIndex};

pub trait NetworkEngine {
    fn on_event(&mut self, local_index: LocalIndex);
    fn on_wake(&mut self);
    fn on_tick(&mut self);
}

pub trait EngineDispatcher {
    fn on_event(&mut self, token: Token);
    fn on_wake(&mut self);
    fn on_tick(&mut self);
}

macro_rules! impl_engine_dispatcher {
    ( $($idx:tt => $name:ident),+ ) => {
        impl<$($name: NetworkEngine),+> EngineDispatcher for ($($name,)+) {
            #[inline(always)]
            fn on_event(&mut self, token: Token) {
                let prefix = token::prefix(token);
                let local_index = token::local_index(token);
                match prefix {
                    $( $idx => self.$idx.on_event(local_index), )+
                    _ => unreachable!("Invalid token prefix"),
                }
            }

            #[inline(always)]
            fn on_tick(&mut self) {
                $( self.$idx.on_tick(); )+
            }

            #[inline(always)]
            fn on_wake(&mut self) {
                $( self.$idx.on_wake(); )+
            }
        }
    }
}

impl_engine_dispatcher!(0 => E0);
impl_engine_dispatcher!(0 => E0, 1 => E1);
impl_engine_dispatcher!(0 => E0, 1 => E1, 2 => E2);
impl_engine_dispatcher!(0 => E0, 1 => E1, 2 => E2, 3 => E3);
impl_engine_dispatcher!(0 => E0, 1 => E1, 2 => E2, 3 => E3, 4 => E4);
impl_engine_dispatcher!(0 => E0, 1 => E1, 2 => E2, 3 => E3, 4 => E4, 5 => E5);
impl_engine_dispatcher!(0 => E0, 1 => E1, 2 => E2, 3 => E3, 4 => E4, 5 => E5, 6 => E6);
impl_engine_dispatcher!(0 => E0, 1 => E1, 2 => E2, 3 => E3, 4 => E4, 5 => E5, 6 => E6, 7 => E7);
