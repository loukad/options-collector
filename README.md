# options-collector
Uses the TD Ameritrade API to collect options data every day

## Build the docker image

Pick up Fly credentials for docker registry:

```sh
fly auth docker
```

Build the image for AMD64 platform:

```sh
docker buildx build --platform linux/amd64 . -t registry.fly.io/options-data:latest --push
```