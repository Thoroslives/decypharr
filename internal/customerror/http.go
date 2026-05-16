package customerror

import (
	"errors"
	"fmt"
)

var HosterUnavailableError = (&Error{
	statusCode: 503,
	err:        errors.New("hoster is unavailable"),
	Code:       "hoster_unavailable",
}).Retryable() // 503 Service Unavailable is transient

// InfringingFileError is the typed seam for a debrid provider rejecting an
// add because the content is DMCA'd / infringing (Real-Debrid returns HTTP
// 451 Unavailable For Legal Reasons, error_code 35 / "infringing_file" on
// /torrents/addMagnet). It is deliberately NEITHER .Retryable() NOR routed
// like too_many_active_downloads: the content is permanently unsatisfiable on
// this provider, so retrying would latch forever. The distinct Code lets an
// upstream handoff key on it later; today it only changes the debug log.
var InfringingFileError = (&Error{
	statusCode: 451,
	err:        errors.New("content is infringing (DMCA) and was rejected by the debrid provider"),
	Code:       "infringing_file",
}).Permanent()

// NewDebridRejectionError wraps any other non-2xx debrid add response in a
// typed error that preserves the HTTP status in both the Code and the
// message (so .Error() still conveys the status, exactly like the prior
// flattened string did). Explicitly permanent / non-retryable so it stays
// behaviourally inert against existing control flow: it is NOT
// too_many_active_downloads, so it never enters the ReQueue branch, and it
// never reports IsRetryable().
func NewDebridRejectionError(statusCode int) *Error {
	return (&Error{
		statusCode: statusCode,
		err:        fmt.Errorf("debrid API error: Status: %d", statusCode),
		Code:       fmt.Sprintf("debrid_status_%d", statusCode),
	}).Permanent()
}

var UsenetSegmentMissingError = &Error{
	statusCode: 404,
	err:        errors.New("usenet segment is missing"),
	Code:       "usenet_segment_missing",
}

var TrafficExceededError = &Error{
	statusCode: 503,
	err:        errors.New("traffic limit exceeded"),
	Code:       "traffic_exceeded",
}

var TorrentNotFoundError = &Error{
	statusCode: 404,
	err:        errors.New("torrent not found"),
	Code:       "torrent_not_found",
}

var TooManyActiveDownloadsError = (&Error{
	statusCode: 509,
	err:        errors.New("too many active downloads"),
	Code:       "too_many_active_downloads",
}).Retryable() // slot exhaustion is transient; retry after backoff
