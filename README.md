# Elastic ZNS Interface

OSDI 2023 paper (https://www.usenix.org/conference/osdi23/presentation/min)

<a id="source"></a>
## Source Code

~~~{.sh}
git clone https://github.com/jaehongm/eZNS
cd eZNS
git submodule update --init
~~~

<a id="prerequisites"></a>
## Prerequisites

eZNS is currently only tested with the small-zone SSD described in the paper. 
You may use any small-zone SSD (regardless of real-hardware or software-emulated),
but it must support all mandatory commands in the ZNS standard (e.g., Zone Reset, Get Zone Info, etc).
Due to the lack of commands, eZNS does not support `bdev_zone_block` in SPDK.

<a id="libraries"></a>
## eZNS bdev module

eZNS is implemented as a bdev module (`bdev_detzone`) in SPDK.
Source codes are located at `./modules/bdev/detzone`.
You can run `rpc.py` to find available RPC commands.

~~~{.sh}
./scripts/rpc.py --help
./scripts/rpc.py bdev_detzone_create --help
./scripts/rpc.py bdev_detzone_delete --help
./scripts/rpc.py bdev_detzone_ns_create --help
./scripts/rpc.py bdev_detzone_ns_delete --help
~~~

<a id="sample_config"></a>
## Sample Configuration JSON

We provide a sample configuration JSON file (`ezns_rdma.json`) used in the paper.
It creates an NVMe-over-Fabrics RDMA target with four eZNS namespaces over a single
small-zone SSD.

## Enable/Disable features

You can enable/disable features in `vbdev_detzone_internal.h`.
Most of the parameters are also configurable, but we recommend you read the paper
and take some time with the codes before making a change.

## Recovery after shutdown

This version DOES NOT guarantee full recovery after shutdown.
(Current recovery procedure does not apply to the latest data structure.)
We highly recommend users perform RESET for all zones before running eZNS.

