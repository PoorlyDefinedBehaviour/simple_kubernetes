## Download the protobuf compiler

```
sudo apt install -y protobuf-compiler
```

## Download etcd

```
curl -o etcd.tar.gz -L https://github.com/etcd-io/etcd/releases/download/v3.5.6/etcd-v3.5.6-linux-amd64.tar.gz; \
mkdir etcd; \
tar -xvzf etcd.tar.gz --strip-components=1 -C etcd
```

## Start etcd

```
./etcd/etcd
```

## Starting the manager

```
cargo r --bin manager -- config -f manager.yml
```

## Starting a worker

```
cargo r --bin worker -- config -f worker.yml
```

## Creating resources

```
cargo r --bin cli -- apply -f 1.yml -e http://[::1]:50051
```