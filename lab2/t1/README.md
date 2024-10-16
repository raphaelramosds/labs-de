# Tarefa 1

## Como executar

```bash
cat sample.txt | python3 mapper.py | python3 reducer.py
```

## Estratégia do MapReduce

- Mapper recebe no stdin o log e vai retornar no stdout

```bash
# Endereço IP 1
8.8.8.8 1
8.8.8.8 1
8.8.8.8 1
192.168.0.104 1
192.168.0.104 1
10.82.90.234 1
```

- Reducer recebe no stdin e retorna no stdout

```bash
# Endereço IP Contagem
8.8.8.8 3
192.168.0.104 2
10.82.90.234 1
```