/*-
 *   BSD LICENSE
 *
 *   Copyright (C) 2008-2012 Daisuke Aoyama <aoyama@peach.ne.jp>.
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
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

#include "spdk/stdinc.h"
#include "iscsi/md5.h"

int md5init(struct spdk_md5ctx *md5ctx)
{
	int rc;

	if (md5ctx == NULL) {
		return -1;
	}

	md5ctx->md5ctx = EVP_MD_CTX_create();
	if (md5ctx->md5ctx == NULL) {
		return -1;
	}

	rc = EVP_DigestInit_ex(md5ctx->md5ctx, EVP_md5(), NULL);
	/* For EVP_DigestInit_ex, 1 == success, 0 == failure. */
	if (rc == 0) {
		EVP_MD_CTX_destroy(md5ctx->md5ctx);
		md5ctx->md5ctx = NULL;
	}
	return rc;
}

int md5final(void *md5, struct spdk_md5ctx *md5ctx)
{
	int rc;

	if (md5ctx == NULL || md5 == NULL) {
		return -1;
	}
	rc = EVP_DigestFinal_ex(md5ctx->md5ctx, md5, NULL);
	EVP_MD_CTX_destroy(md5ctx->md5ctx);
	md5ctx->md5ctx = NULL;
	return rc;
}

int md5update(struct spdk_md5ctx *md5ctx, const void *data, size_t len)
{
	int rc;

	if (md5ctx == NULL) {
		return -1;
	}
	if (data == NULL || len == 0) {
		return 0;
	}
	rc = EVP_DigestUpdate(md5ctx->md5ctx, data, len);
	return rc;
}
