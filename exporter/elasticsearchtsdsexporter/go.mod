module github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter

go 1.27.0

replace github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension => ../../extension/oneconfextension

require (
	github.com/bytedance/sonic v1.15.5
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/klauspost/compress v1.20.4
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage v0.166.0
	github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.12.1
	gitlab.rip/golang/platform-tech-services/one_configuration v1.24.1
	gitlab.rip/platform/go-starter/v3 v3.5.4
	go.opentelemetry.io/collector/component v1.72.0
	go.opentelemetry.io/collector/component/componenttest v0.166.0
	go.opentelemetry.io/collector/config/confighttp v1.72.0
	go.opentelemetry.io/collector/config/configoptional v1.72.0
	go.opentelemetry.io/collector/config/configretry v1.72.0
	go.opentelemetry.io/collector/config/configstorage v0.166.0
	go.opentelemetry.io/collector/config/configtls v1.72.0
	go.opentelemetry.io/collector/confmap v1.72.0
	go.opentelemetry.io/collector/consumer/consumererror v0.166.0
	go.opentelemetry.io/collector/exporter v1.72.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.166.0
	go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper v0.166.0
	go.opentelemetry.io/collector/exporter/exportertest v0.166.0
	go.opentelemetry.io/collector/featuregate v1.72.0
	go.opentelemetry.io/collector/pdata v1.72.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/metric v1.47.0
	go.opentelemetry.io/otel/sdk/metric v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/Microsoft/go-winio v0.6.4 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic/loader v0.5.3 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cenkalti/backoff/v7 v7.0.3 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20260902202739-8c9c2d1005f4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/go-tpm v0.9.9-0.20260124013517-8f8f42cba0de // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/gurkankaymak/hocon v1.2.23 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.8 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-secure-stdlib/parseutil v0.2.0 // indirect
	github.com/hashicorp/go-secure-stdlib/strutil v0.1.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hashicorp/hcl v1.0.1-vault-7 // indirect
	github.com/hashicorp/vault/api v1.22.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/knadh/koanf/maps v0.1.3 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.4.1 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/samber/lo v1.52.0 // indirect
	github.com/senseyeio/duration v0.0.0-20180430131211-7c2a214ada46 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	gitlab.rip/golang/platform-tech-services/one_metrics v0.0.18 // indirect
	gitlab.rip/platform/go-01-vault.git v0.0.0-20251023134140-f4fe8dafd68b // indirect
	go.elastic.co/ecszerolog v0.2.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/client v1.72.0 // indirect
	go.opentelemetry.io/collector/config/configauth v1.72.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.72.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v1.72.0 // indirect
	go.opentelemetry.io/collector/config/confignet v1.72.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.72.0 // indirect
	go.opentelemetry.io/collector/consumer v1.72.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror/xconsumererror v0.166.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.166.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.166.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.166.0 // indirect
	go.opentelemetry.io/collector/extension v1.72.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.72.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.166.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.166.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.166.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.166.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.166.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.166.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.72.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.166.0 // indirect
	go.opentelemetry.io/collector/receiver v1.72.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.166.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.166.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.64.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.64.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.39.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.39.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.39.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.39.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.63.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260817212433-ac3dfec99bb1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
	google.golang.org/grpc v1.85.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
