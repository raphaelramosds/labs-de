# Tarefa 2

## Como executar

```bash
ls samples | python3 mapper.py | python3 reducer.py
```

## Estratégia do MapReduce

- Mapper recebe no stdin e vai retornar no stdout

```bash
# Nome da cidade Temperatura
BRASILIA 21.9
BRASILIA 21.9
BRAZLANDIA 21.6
BRAZLANDIA 21.6
AGUAS EMENDADAS 20.9
```

- Reducer recebe no stdin e retorna no stdout

```bash
# Nome da cidade Média das temperaturas
BRASILIA 22.5
BRAZLANDIA 22.8
AGUAS EMENDADAS 20.9
```