use journal::journal::Journal;

pub trait NetworkEngine {
    fn on_tick(&mut self, journal: &mut Journal);
}

pub trait EngineDispatcher {
    fn on_tick(&mut self, journal: &mut Journal);
}

macro_rules! impl_engine_dispatcher {
    ( $($idx:tt => $name:ident),+ ) => {
        impl<$($name: NetworkEngine),+> EngineDispatcher for ($($name,)+) {
            #[inline(always)]
            fn on_tick(&mut self, journal: &mut Journal) {
                $( self.$idx.on_tick(journal); )+
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
