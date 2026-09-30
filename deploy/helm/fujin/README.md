# Fujin Helm Chart

## Modes

### Standalone (default)

Fujin runs as a separate Deployment with its own Service.

```bash
helm install fujin ./deploy/helm/fujin
```

### Sidecar

Generates a ConfigMap for Fujin config. Add the sidecar container to your own Deployment using the rendered template as a reference, or include the chart as a subchart.

```bash
helm install fujin ./deploy/helm/fujin --set mode=sidecar
```

## Values

See [values.yaml](values.yaml) for all configuration options.

The chart defaults to the immutable Fujin image tag matching `appVersion`. Override `image.repository` or `image.tag` when publishing through a different registry.

## NATS JetStream connector

The chart does not provision broker resources. Create a JetStream stream and durable explicit-ACK pull consumer separately before enabling a `type: nats` connector in `config.connectors` ([configuration](../../../docs/configuration.md#nats-jetstream-instances-and-routes)). The default Docker image links Kafka but not NATS; build and select a custom image with `fujin-connector-nats` statically registered ([composition](../../../docs/extending.md#compose-with-cargo-fujin)). Mount a `.creds` file from a Kubernetes Secret at a path readable by the Fujin container's numeric user; set `common.credentials_file` to that path. Never put credential contents into `config`, which is stored in a ConfigMap. Broker connectivity is checked on first use, not by the HTTP readiness probe.
