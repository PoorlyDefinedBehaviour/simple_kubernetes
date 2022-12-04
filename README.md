## Starting the manager

```
cargo r --bin manager
```

## Starting a worker

```
cargo r --bin worker -- config -f worker.yml
```

## Creating resources

```
cargo r --bin cli -- apply -f 1.yml
```
