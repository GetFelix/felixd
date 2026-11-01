//! Generated from `wit/0.2` and `wit/0.3`, one module per contract version.
//! `step` and `state` differ between them only in `record-in`, so everything
//! else is written once and expanded in each.

use wasmtime::Store;
use wasmtime::StoreLimits;
use wasmtime::component::{Component, Linker};

use crate::Input;
use crate::workload::{Called, Stepped, stepped_from};

macro_rules! contract {
    ($version:ident, $dir:literal, $record_in:expr) => {
        mod $version {
            use super::*;

            wasmtime::component::bindgen!({
                path: $dir,
                world: "workload",
            });

            use exports::felixd::workload::step as wit;

            pub(super) fn instantiate(
                store: &mut Store<StoreLimits>,
                component: &Component,
                linker: &Linker<StoreLimits>,
            ) -> wasmtime::Result<Workload> {
                Workload::instantiate(store, component, linker)
            }

            pub(super) fn step(
                guest: &Workload,
                store: &mut Store<StoreLimits>,
                input: &Input<'_>,
            ) -> wasmtime::Result<Called<Stepped>> {
                let record_in: fn(&Input<'_>) -> wit::RecordIn = $record_in;
                let input = wit::Input {
                    source: wit::Source {
                        stream: input.stream.to_owned(),
                        incarnation: input.incarnation,
                    },
                    offset: input.offset,
                    timestamp_micros: input.timestamp_micros,
                    body: wit::InputBody::Record(record_in(input)),
                };
                let stepped = guest.felixd_workload_step().call_step(store, &input)?;
                Ok(stepped_from(
                    stepped
                        .map(|outputs| outputs.into_iter().map(output).collect())
                        .map_err(step_error),
                ))
            }

            fn output(output: wit::Output) -> crate::workload::Emitted {
                match output {
                    wit::Output::Record(addressed) => crate::workload::Emitted::Record {
                        name: addressed.name,
                        key: addressed.key,
                        payload: addressed.payload,
                    },
                    wit::Output::Request(request) => {
                        crate::workload::Emitted::Request { kind: request.kind }
                    }
                }
            }

            fn step_error(error: wit::StepError) -> crate::workload::Refused {
                match error {
                    wit::StepError::Reject(error) => crate::workload::Refused::Reject {
                        code: error.code,
                        detail: error.detail,
                    },
                    wit::StepError::Poison(reason) => crate::workload::Refused::Poison(reason),
                }
            }

            pub(super) fn snapshot(
                guest: &Workload,
                store: &mut Store<StoreLimits>,
            ) -> wasmtime::Result<u32> {
                guest.felixd_workload_state().call_snapshot(store)
            }

            pub(super) fn snapshot_chunk(
                guest: &Workload,
                store: &mut Store<StoreLimits>,
                index: u32,
            ) -> wasmtime::Result<Vec<u8>> {
                guest.felixd_workload_state().call_snapshot_chunk(store, index)
            }

            pub(super) fn restore(
                guest: &Workload,
                store: &mut Store<StoreLimits>,
                chunks: u32,
            ) -> wasmtime::Result<()> {
                guest.felixd_workload_state().call_restore(store, chunks)
            }

            pub(super) fn restore_chunk(
                guest: &Workload,
                store: &mut Store<StoreLimits>,
                index: u32,
                chunk: &[u8],
            ) -> wasmtime::Result<()> {
                guest
                    .felixd_workload_state()
                    .call_restore_chunk(store, index, chunk)
            }

            pub(super) fn inspect(
                guest: &Workload,
                store: &mut Store<StoreLimits>,
            ) -> wasmtime::Result<String> {
                guest.felixd_workload_state().call_inspect(store)
            }
        }
    };
}

contract!(v02, "../../wit/0.2", |input| wit::RecordIn {
    key: input.key.map(<[u8]>::to_vec),
    payload: input.payload.to_vec(),
});

contract!(v03, "../../wit/0.3", |input| wit::RecordIn {
    key: input.key.map(<[u8]>::to_vec),
    payload: input.payload.to_vec(),
    publisher: input.publisher.map(str::to_owned),
});

/// The contract version a component implements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Version {
    V02,
    V03,
}

impl Version {
    /// The `step` and `state` interfaces a workload of this version exports.
    pub(crate) const fn interfaces(self) -> (&'static str, &'static str) {
        match self {
            Version::V02 => ("felixd:workload/step@0.2.0", "felixd:workload/state@0.2.0"),
            Version::V03 => ("felixd:workload/step@0.3.0", "felixd:workload/state@0.3.0"),
        }
    }
}

/// An instantiated workload of either version.
pub(crate) enum Guest {
    V02(v02::Workload),
    V03(v03::Workload),
}

impl Guest {
    pub(crate) fn instantiate(
        version: Version,
        store: &mut Store<StoreLimits>,
        component: &Component,
        linker: &Linker<StoreLimits>,
    ) -> wasmtime::Result<Guest> {
        Ok(match version {
            Version::V02 => Guest::V02(v02::instantiate(store, component, linker)?),
            Version::V03 => Guest::V03(v03::instantiate(store, component, linker)?),
        })
    }

    pub(crate) fn step(
        &self,
        store: &mut Store<StoreLimits>,
        input: &Input<'_>,
    ) -> wasmtime::Result<Called<Stepped>> {
        match self {
            Guest::V02(guest) => v02::step(guest, store, input),
            Guest::V03(guest) => v03::step(guest, store, input),
        }
    }

    pub(crate) fn snapshot(&self, store: &mut Store<StoreLimits>) -> wasmtime::Result<u32> {
        match self {
            Guest::V02(guest) => v02::snapshot(guest, store),
            Guest::V03(guest) => v03::snapshot(guest, store),
        }
    }

    pub(crate) fn snapshot_chunk(
        &self,
        store: &mut Store<StoreLimits>,
        index: u32,
    ) -> wasmtime::Result<Vec<u8>> {
        match self {
            Guest::V02(guest) => v02::snapshot_chunk(guest, store, index),
            Guest::V03(guest) => v03::snapshot_chunk(guest, store, index),
        }
    }

    pub(crate) fn restore(
        &self,
        store: &mut Store<StoreLimits>,
        chunks: u32,
    ) -> wasmtime::Result<()> {
        match self {
            Guest::V02(guest) => v02::restore(guest, store, chunks),
            Guest::V03(guest) => v03::restore(guest, store, chunks),
        }
    }

    pub(crate) fn restore_chunk(
        &self,
        store: &mut Store<StoreLimits>,
        index: u32,
        chunk: &[u8],
    ) -> wasmtime::Result<()> {
        match self {
            Guest::V02(guest) => v02::restore_chunk(guest, store, index, chunk),
            Guest::V03(guest) => v03::restore_chunk(guest, store, index, chunk),
        }
    }

    pub(crate) fn inspect(&self, store: &mut Store<StoreLimits>) -> wasmtime::Result<String> {
        match self {
            Guest::V02(guest) => v02::inspect(guest, store),
            Guest::V03(guest) => v03::inspect(guest, store),
        }
    }
}
