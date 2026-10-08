# Shared by the scripts: load cluster.env and build a resolved Kafka client config.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# cluster.env is plain KEY=VALUE (values may hold | and *), so read it line by line instead of sourcing.
ENV_FILE=cluster.env; [ -f "$ENV_FILE" ] || ENV_FILE=cluster.env.example
while IFS= read -r line; do
  case $line in ''|\#*) ;; *) export "${line%%=*}=${line#*=}" ;; esac
done < "$ENV_FILE"
PROPS=${SQ_KAFKA_PROPS:-config/kafka.properties}
NET=${KAFKA_DOCKER_NETWORK:-bridge}
CLI_IMAGE=${KAFKA_CLI_IMAGE:-apache/kafka:3.9.1}

resolve() { perl -pe 's/\$\{env:(\w+)(?::-([^}]*))?\}/defined $ENV{$1} ? $ENV{$1} : defined $2 ? $2 : die "env var $1 not set (referenced in $ARGV)\n"/ge'; }
get() { grep -E "^$1=" "$PROPS" | head -1 | cut -d= -f2- | resolve; }

# Client props = everything except sq.* keys, with ${env:NAME} resolved.
client_props() { grep -vE "^(#|sq\.|$)" "$PROPS" | resolve > "$1"; chmod 644 "$1"; }

# Run a Kafka CLI tool from a container on the Kafka network. Usage: kcli <props-file> <tool.sh> args...
kcli() {
  local props=$1 tool=$2; shift 2
  docker run --rm --network "$NET" --add-host=host.docker.internal:host-gateway \
    -v "$props:/client.properties:ro" "$CLI_IMAGE" /opt/kafka/bin/"$tool" "$@"
}
