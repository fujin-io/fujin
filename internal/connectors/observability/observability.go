package observability

import (
	"github.com/fujin-io/fujin/public/connectors/reader"
	"github.com/fujin-io/fujin/public/connectors/writer"
)

var (
	OtelWriterWrapper = func(w writer.Writer, connectorName string) writer.Writer {
		return w
	}

	OtelReaderWrapper = func(r reader.Reader, connectorName string) reader.Reader {
		return r
	}

	MetricsWriterWrapper = func(w writer.Writer, connectorName string) writer.Writer {
		return w
	}

	MetricsReaderWrapper = func(r reader.Reader, connectorName string) reader.Reader {
		return r
	}
)
