//! Boot epoch: a random `u64` drawn once at daemon startup, constant across every connection
//! served by this running instance (SPEC §19.1), and injectable for deterministic tests.

/// A daemon-instance-lifetime random value, handed to every connected client via HELLO_ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootEpoch(pub u64);

/// A source of the boot epoch. The real implementation draws from `getrandom` once; tests
/// inject a `FixedEpoch` to assert determinism without depending on process entropy.
pub trait EpochSource {
    fn epoch(&self) -> BootEpoch;
}

/// The real `EpochSource`: fills 8 bytes from the OS CSPRNG via `getrandom` and interprets
/// them as a little-endian `u64`.
#[derive(Debug, Default, Clone, Copy)]
pub struct RandomEpoch;

impl EpochSource for RandomEpoch {
    fn epoch(&self) -> BootEpoch {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).expect("getrandom must succeed to draw a boot epoch");
        BootEpoch(u64::from_le_bytes(buf))
    }
}

/// A fixed, injectable `EpochSource` for tests.
#[derive(Debug, Clone, Copy)]
pub struct FixedEpoch(pub u64);

impl EpochSource for FixedEpoch {
    fn epoch(&self) -> BootEpoch {
        BootEpoch(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_epoch_is_stable() {
        let src = FixedEpoch(42);
        assert_eq!(src.epoch(), BootEpoch(42));
        assert_eq!(src.epoch(), src.epoch());
    }

    #[test]
    fn random_epoch_produces_a_value() {
        // Not a determinism assertion (that's what FixedEpoch is for) — just confirms the real
        // getrandom-backed source is callable and doesn't panic.
        let src = RandomEpoch;
        let _ = src.epoch();
    }
}
