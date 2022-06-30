/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "vbdev_congctrl_internal.h"
#include "vbdev_congctrl.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk_internal/assert.h"

struct rpc_construct_congctrl {
	char *base_bdev_name;
	char *name;
	uint32_t num_pu;
};

static void
free_rpc_construct_congctrl(struct rpc_construct_congctrl *r)
{
	free(r->base_bdev_name);
	free(r->name);
}

static const struct spdk_json_object_decoder rpc_construct_congctrl_decoders[] = {
	{"base_bdev_name", offsetof(struct rpc_construct_congctrl, base_bdev_name), spdk_json_decode_string},
	{"name", offsetof(struct rpc_construct_congctrl, name), spdk_json_decode_string},
	{"num_put", offsetof(struct rpc_construct_congctrl, num_pu), spdk_json_decode_uint32},
};

static void
rpc_bdev_congctrl_create(struct spdk_jsonrpc_request *request,
		      const struct spdk_json_val *params)
{
	struct rpc_construct_congctrl req = {NULL};
	struct spdk_json_write_ctx *w;
	int rc;

	if (spdk_json_decode_object(params, rpc_construct_congctrl_decoders,
				    SPDK_COUNTOF(rpc_construct_congctrl_decoders),
				    &req)) {
		SPDK_DEBUGLOG(vbdev_congctrl, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = create_congctrl_disk(req.base_bdev_name, req.name, req.num_pu);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, req.name);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_construct_congctrl(&req);
}
SPDK_RPC_REGISTER("bdev_congctrl_create", rpc_bdev_congctrl_create, SPDK_RPC_RUNTIME)

struct rpc_delete_congctrl {
	char *name;
};

static void
free_rpc_delete_congctrl(struct rpc_delete_congctrl *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_delete_congctrl_decoders[] = {
	{"name", offsetof(struct rpc_delete_congctrl, name), spdk_json_decode_string},
};

static void
rpc_bdev_congctrl_delete_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	spdk_jsonrpc_send_bool_response(request, bdeverrno == 0);
}

static void
rpc_bdev_congctrl_delete(struct spdk_jsonrpc_request *request,
		      const struct spdk_json_val *params)
{
	struct rpc_delete_congctrl req = {NULL};
	struct spdk_bdev *bdev;

	if (spdk_json_decode_object(params, rpc_delete_congctrl_decoders,
				    SPDK_COUNTOF(rpc_delete_congctrl_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	delete_congctrl_disk(bdev, rpc_bdev_congctrl_delete_cb, request);

cleanup:
	free_rpc_delete_congctrl(&req);
}
SPDK_RPC_REGISTER("bdev_congctrl_delete", rpc_bdev_congctrl_delete, SPDK_RPC_RUNTIME)

struct rpc_construct_congctrl_ns {
	char *ns_name;
	char *ctrl_name;
	uint32_t zone_array_size;
	uint32_t stripe_size;
	uint32_t block_align;
	uint64_t start_base_zone;
	uint64_t num_base_zones;
};

static void
free_rpc_construct_congctrl_ns(struct rpc_construct_congctrl_ns *r)
{
	free(r->ns_name);
	free(r->ctrl_name);
}

static const struct spdk_json_object_decoder rpc_construct_congctrl_ns_decoders[] = {
	{"ns_name", offsetof(struct rpc_construct_congctrl_ns, ns_name), spdk_json_decode_string},
	{"ctrl_name", offsetof(struct rpc_construct_congctrl_ns, ctrl_name), spdk_json_decode_string},
	{"zone_array_size", offsetof(struct rpc_construct_congctrl_ns, zone_array_size), spdk_json_decode_uint32, true},
	{"stripe_size", offsetof(struct rpc_construct_congctrl_ns, stripe_size), spdk_json_decode_uint32, true},
	{"block_align", offsetof(struct rpc_construct_congctrl_ns, block_align), spdk_json_decode_uint32, true},
	{"start_base_zone", offsetof(struct rpc_construct_congctrl_ns, start_base_zone), spdk_json_decode_uint64, false},
	{"num_base_zones", offsetof(struct rpc_construct_congctrl_ns, num_base_zones), spdk_json_decode_uint64, false},
};

static void
rpc_bdev_congctrl_ns_create(struct spdk_jsonrpc_request *request,
		      const struct spdk_json_val *params)
{
	struct rpc_construct_congctrl_ns req = {NULL};
	struct spdk_json_write_ctx *w;
	int rc;

	if (spdk_json_decode_object(params, rpc_construct_congctrl_ns_decoders,
				    SPDK_COUNTOF(rpc_construct_congctrl_ns_decoders),
				    &req)) {
		SPDK_DEBUGLOG(vbdev_congctrl, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = create_congctrl_ns(req.ctrl_name, req.ns_name,
							 req.zone_array_size, req.stripe_size, req.block_align,
							 req.start_base_zone, req.num_base_zones);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, req.ns_name);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_construct_congctrl_ns(&req);
}
SPDK_RPC_REGISTER("bdev_congctrl_ns_create", rpc_bdev_congctrl_ns_create, SPDK_RPC_RUNTIME)

struct rpc_delete_congctrl_ns {
	char *ns_name;
	char *ctrl_name;
};

static void
free_rpc_delete_congctrl_ns(struct rpc_delete_congctrl_ns *req)
{
	free(req->ns_name);
	free(req->ctrl_name);
}

static const struct spdk_json_object_decoder rpc_delete_congctrl_ns_decoders[] = {
	{"ns_name", offsetof(struct rpc_delete_congctrl_ns, ns_name), spdk_json_decode_string},
	{"ctrl_name", offsetof(struct rpc_delete_congctrl_ns, ctrl_name), spdk_json_decode_string},
};

static void
rpc_bdev_congctrl_ns_delete_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	spdk_jsonrpc_send_bool_response(request, bdeverrno == 0);
}

static void
rpc_bdev_congctrl_ns_delete(struct spdk_jsonrpc_request *request,
		      const struct spdk_json_val *params)
{
	struct rpc_delete_congctrl_ns req = {NULL};
	struct spdk_bdev *bdev;

	if (spdk_json_decode_object(params, rpc_delete_congctrl_ns_decoders,
				    SPDK_COUNTOF(rpc_delete_congctrl_ns_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.ctrl_name);
	if (bdev == NULL) {
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	delete_congctrl_ns(bdev, rpc_bdev_congctrl_ns_delete_cb, request);

cleanup:
	free_rpc_delete_congctrl_ns(&req);
}
SPDK_RPC_REGISTER("bdev_congctrl_ns_delete", rpc_bdev_congctrl_ns_delete, SPDK_RPC_RUNTIME)
