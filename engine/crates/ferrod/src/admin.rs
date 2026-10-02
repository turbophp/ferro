//! **The admin service's verbs and their D15 authorization (M2-C3-7b, SPEC §7.6 + D15).**
//!
//! `ADMIN = 5` methods are authorized HERE, in the session layer, before a handler is spawned: a
//! refused verb never reaches a pool, never checks out a connection and never touches the
//! filesystem. Two facts decide it, both held by the session and neither supplied by the client:
//!
//! * the peer's **kernel-attested uid** — `SO_PEERCRED` on the session's own Unix socket, read once
//!   when the session starts. `SO_PEERCRED` reports the credentials of the process that CREATED the
//!   connection and never changes for the life of the socket, so reading it once is the same
//!   answer as reading it per verb; what matters is that it comes from the KERNEL, never the wire.
//! * the engine's own configuration: `FERRO_ADMIN_UIDS`.
//!
//! The rule (D15, verbatim in effect):
//!
//! | verb class | requires |
//! |---|---|
//! | READ | an attested uid (the accept-time `FERRO_ALLOW_UIDS` gate already admitted it) |
//! | OPERATE | an attested uid that is IN `FERRO_ADMIN_UIDS` — **empty means OPERATE is disabled** |
//!
//! A connection with no attested uid is refused every admin verb. Every verb's class is fixed by
//! an exhaustive `match` in [`AdminVerb::class`], so adding a method to `[methods.admin]` cannot
//! compile into a verb with no class — the "forgot to gate it" state is unexpressible.
//!
//! **M2 builds no READ verb.** `BACKUP` (OPERATE) is the admin service's only consumer in M2; a READ
//! verb such as pool state already has a reader in Prometheus (§22.2 (bs)/(bv)), and its admin-side
//! consumer is `ferro top`, which is M4. The READ arm of the rule is implemented and unit-tested so
//! the first READ verb inherits it rather than inventing one.

use ferro_proto::consts::{errc, method_admin};
use ferro_proto::messages::ErrorPayload;

use crate::config::Config;

/// One admin method this build serves. Exhaustive by construction: a `[methods.admin]` id that is
/// not mapped here routes to `Unsupported`, never to a handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminVerb {
    /// `BACKUP`: a consistent snapshot of a SQLite pool's database (§7.6). OPERATE — it produces a
    /// full copy of the data.
    Backup,
}

/// D15's two verb classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerbClass {
    /// Pool and engine state, bounded by the redaction contract. Any admitted peer.
    Read,
    /// Writes, changes engine state, or returns protected material. `FERRO_ADMIN_UIDS` members only.
    Operate,
}

impl AdminVerb {
    /// The verb for an `ADMIN` method id, if this build serves it.
    pub fn from_method(method: u16) -> Option<Self> {
        match method {
            method_admin::BACKUP => Some(Self::Backup),
            _ => None,
        }
    }

    /// The verb's D15 class. Exhaustive: a new variant must be classified to compile.
    pub fn class(self) -> VerbClass {
        match self {
            Self::Backup => VerbClass::Operate,
        }
    }

    /// The registry name, for refusal messages and logs.
    pub fn name(self) -> &'static str {
        match self {
            Self::Backup => "BACKUP",
        }
    }
}

/// Decide whether a peer may run `verb`. `peer_uid` is the session's kernel-attested uid, `None`
/// when the socket could not attest one. The refusal is a ready-to-send `Forbidden` terminal.
///
/// **The two OPERATE refusals read IDENTICALLY on the wire** (M2-C3-7b review): "OPERATE is
/// disabled" and "this uid is not a member" used to be different messages, which told any admitted
/// peer whether OPERATE was enabled at all — configuration it is not entitled to. The REASON is
/// logged server-side, where the operator who has to fix it will look.
pub fn authorize(
    peer_uid: Option<u32>,
    config: &Config,
    verb: AdminVerb,
) -> Result<(), ErrorPayload> {
    decide(peer_uid, config, verb.class()).map_err(|refusal| {
        tracing::warn!(
            verb = verb.name(),
            peer_uid,
            reason = refusal.reason(),
            "admin verb refused (SPEC D15)"
        );
        refusal.payload(verb.name())
    })
}

/// Why D15 refused a verb. The reason is for the LOG; [`Refusal::payload`] is what the peer sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The connection has no kernel-attested uid.
    Unattested,
    /// An OPERATE verb while `FERRO_ADMIN_UIDS` is empty.
    OperateDisabled,
    /// An OPERATE verb from a uid that is not in `FERRO_ADMIN_UIDS`.
    NotAdmin,
}

impl Refusal {
    fn reason(self) -> &'static str {
        match self {
            Self::Unattested => "no kernel-attested peer uid",
            Self::OperateDisabled => "FERRO_ADMIN_UIDS is empty, which disables every OPERATE verb",
            Self::NotAdmin => "the peer uid is not in FERRO_ADMIN_UIDS",
        }
    }

    fn payload(self, name: &str) -> ErrorPayload {
        forbidden(match self {
            Self::Unattested => format!(
                "admin verb {name} refused: this connection has no kernel-attested peer uid (SPEC \
                 D15 serves admin verbs only where SO_PEERCRED can attest one)"
            ),
            Self::OperateDisabled | Self::NotAdmin => format!(
                "admin verb {name} is an OPERATE verb and this peer is not authorized for OPERATE \
                 verbs (SPEC D15: the peer's uid must be in FERRO_ADMIN_UIDS)"
            ),
        })
    }
}

/// The D15 rule over a verb CLASS — the one implementation, so the READ arm (which no M2 verb
/// reaches) is the same code a future READ verb will run, and is tested directly.
fn decide(peer_uid: Option<u32>, config: &Config, class: VerbClass) -> Result<(), Refusal> {
    let Some(uid) = peer_uid else {
        return Err(Refusal::Unattested);
    };
    match class {
        VerbClass::Read => Ok(()),
        VerbClass::Operate if config.admin_uids.is_empty() => Err(Refusal::OperateDisabled),
        VerbClass::Operate if config.admin_uids.contains(&uid) => Ok(()),
        VerbClass::Operate => Err(Refusal::NotAdmin),
    }
}

/// A `Forbidden` (NonRetryable) terminal payload: refused by the engine's own access policy.
pub fn forbidden(message: String) -> ErrorPayload {
    ErrorPayload {
        code: errc::FORBIDDEN,
        branch: errc::FORBIDDEN_BRANCH,
        sqlstate: None,
        errno: None,
        message,
        detail: None,
        retry_after_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_admins(uids: &[u32]) -> Config {
        Config {
            admin_uids: uids.to_vec(),
            ..Config::default()
        }
    }

    #[test]
    fn every_admin_method_in_the_registry_is_a_served_verb() {
        // The registry is the source of truth (charter rule 2): every `[methods.admin]` id must map
        // to a verb, or a method would ship that the session routes to `Unsupported` forever.
        for &(name, id) in method_admin::ALL {
            let verb = AdminVerb::from_method(id)
                .unwrap_or_else(|| panic!("[methods.admin] {name} = {id} has no AdminVerb"));
            assert_eq!(verb.name(), name, "the verb's name is the registry's");
        }
        assert_eq!(AdminVerb::from_method(0), None);
        assert_eq!(AdminVerb::from_method(0xFFFF), None);
    }

    #[test]
    fn operate_is_disabled_while_ferro_admin_uids_is_empty() {
        // THE default. Including for the daemon's own uid: D15 has no implicit member, because
        // `uid_allowed` admits the daemon's own uid by default and every same-uid PHP worker would
        // otherwise hold every OPERATE verb.
        let cfg = config_with_admins(&[]);
        let own = Config::own_uid();
        assert_eq!(
            decide(Some(own), &cfg, VerbClass::Operate),
            Err(Refusal::OperateDisabled)
        );
        let err = authorize(Some(own), &cfg, AdminVerb::Backup).unwrap_err();
        assert_eq!(err.code, errc::FORBIDDEN);
        assert_eq!(err.branch, errc::FORBIDDEN_BRANCH);
    }

    #[test]
    fn operate_requires_membership_and_a_member_is_allowed() {
        let cfg = config_with_admins(&[1000, 1001]);
        assert!(authorize(Some(1001), &cfg, AdminVerb::Backup).is_ok());
        assert_eq!(
            decide(Some(1002), &cfg, VerbClass::Operate),
            Err(Refusal::NotAdmin)
        );
        let err = authorize(Some(1002), &cfg, AdminVerb::Backup).unwrap_err();
        assert_eq!(err.code, errc::FORBIDDEN);
    }

    /// The two OPERATE refusals are indistinguishable to the peer — neither the configured list nor
    /// whether OPERATE is enabled at all leaks — while the log reasons differ.
    #[test]
    fn the_two_operate_refusals_read_identically_on_the_wire() {
        let disabled =
            authorize(Some(1002), &config_with_admins(&[]), AdminVerb::Backup).unwrap_err();
        let not_member =
            authorize(Some(1002), &config_with_admins(&[1000]), AdminVerb::Backup).unwrap_err();
        assert_eq!(disabled, not_member);
        assert!(
            !not_member.message.contains("1000"),
            "{}",
            not_member.message
        );
        assert!(
            !not_member.message.contains("1002"),
            "{}",
            not_member.message
        );
        assert_ne!(
            Refusal::OperateDisabled.reason(),
            Refusal::NotAdmin.reason()
        );
    }

    #[test]
    fn no_attested_uid_is_refused_every_class() {
        let cfg = config_with_admins(&[0, 1000]);
        // Exhaustive over the verbs this build serves, derived from the registry: an unattested
        // peer is refused EVERY one, whatever its class.
        for &(_, id) in method_admin::ALL {
            let verb = AdminVerb::from_method(id).expect("every admin method is a verb");
            let err = authorize(None, &cfg, verb).unwrap_err();
            assert_eq!(err.code, errc::FORBIDDEN);
            assert!(err.message.contains("kernel-attested"), "{}", err.message);
        }
        // The READ arm, which no M2 verb exercises end to end, is pinned at the rule itself.
        assert_eq!(
            decide(None, &cfg, VerbClass::Read),
            Err(Refusal::Unattested)
        );
    }

    #[test]
    fn read_needs_only_an_attested_uid() {
        // READ does not consult FERRO_ADMIN_UIDS: an admitted peer outside it, with the list empty
        // or populated, may read.
        assert_eq!(
            decide(Some(4242), &config_with_admins(&[]), VerbClass::Read),
            Ok(())
        );
        assert_eq!(
            decide(Some(4242), &config_with_admins(&[1000]), VerbClass::Read),
            Ok(())
        );
    }
}
