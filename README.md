Rodar o Zookeeper em um container expondo a porta 2181:

```
docker run --name some-zookeeper -p 2181:2181 --restart always -d zookeeper
```

Conectar ao container com o Zookeeper CLI:

```
docker run -it --rm --link some-zookeeper:zookeeper zookeeper zkCli.sh -server zookeeper
```

Rodar o programa:

``` java
java Executor localhost znode arquivo programa [args ...]
```