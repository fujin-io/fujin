package server

import (
	"github.com/fujin-io/fujin/internal/api/fujin/v1/proto/response"
)

func ProduceResponseSuccess(buf []byte, cID []byte) []byte {
	buf = append(buf, byte(response.RESP_CODE_PRODUCE))
	buf = append(buf, cID...)
	return append(buf, response.ERR_CODE_NO)
}
