module github.com/aliyun/cachefs

go 1.25.0

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/containerd/ttrpc v1.2.7
	github.com/envoyproxy/protoc-gen-validate v1.3.3
	github.com/erikdubbelboer/gspt v0.0.0-20210805194459-ce36a5128377
	github.com/eryugey/consistent v1.0.901
	github.com/eryugey/go-fuse/v2 v2.1.1-0.20260623160106-769c39fe00b3
	github.com/eryugey/serf v0.10.904
	github.com/google/gops v0.3.22
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/logutils v1.0.0
	github.com/hashicorp/memberlist v0.5.1
	github.com/juicedata/godaemon v0.0.0-20210629045518-3da5144a127d
	github.com/juju/ratelimit v1.0.1
	github.com/mattn/go-isatty v0.0.14
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/pyroscope-io/client v0.2.1
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.2
	github.com/urfave/cli/v2 v2.4.0
	github.com/vbauerster/mpb/v7 v7.0.3
	github.com/willf/bitset v1.1.11
	golang.org/x/sync v0.22.0
	golang.org/x/sys v0.47.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.5 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/miekg/dns v1.1.56 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	stathat.com/c/consistent v1.0.0 // indirect
)

replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible

replace github.com/vbauerster/mpb/v7 v7.0.3 => github.com/juicedata/mpb/v7 v7.0.4-0.20220719014258-68df1356cfba

replace google.golang.org/grpc v1.43.0 => google.golang.org/grpc v1.29.0
