# Roteiro de validação — Abordagem A

## Objetivo

Validar o fluxo de geração de conexões fictícias e sua persistência assíncrona:

`POST /connections/mock` → producer FastAPI → tópico Kafka `connections.mock` no Redpanda → consumer `connections-persistor` → tabela `ecommerce.connections` no PostgreSQL.

Cada requisição aceita de 1 a 200 registros; se `count` for omitido, são gerados 200. Os dados de senha são fictícios e `hashpass` deve conter apenas um hash PBKDF2, nunca a senha em texto puro.

## Pré-requisitos

- Docker Desktop ativo e Docker Compose v2 disponível.
- Arquivo `.env` configurado com as variáveis requeridas pelo projeto.
- Acesso ao Docker Hub para baixar imagens, se ainda não estiverem em cache.

## 1. Subir e verificar os serviços

Na raiz do projeto, execute no PowerShell:

```powershell
docker compose up -d --build backend-migrate backend-api ecommerce-worker
```

O Compose iniciará também as dependências (`postgres` e `redpanda`). Verifique:

```powershell
docker compose ps -a
```

Esperado:

- `postgres`, `redpanda`, `backend-api` e `ecommerce-worker` em execução/saudáveis.
- `backend-migrate` finalizado com código de saída zero.

Se a migration já tiver sido aplicada, é normal que a tarefa termine sem alterações adicionais.

## 2. Validar a migration e a tabela

Confira a revisão atual do Alembic:

```powershell
docker compose run --rm backend-migrate alembic current
```

Esperado: revisão `0005_connections`.

Confira colunas e restrições no PostgreSQL:

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "\d ecommerce.connections"
```

Esperado: `userid` (UUID, chave primária), `login` (único e não nulo), `hashpass` (não nulo) e `date_access` (timestamp com timezone, não nulo).

## 3. Registrar a contagem inicial

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT count(*) AS total_inicial FROM ecommerce.connections;"
```

Anote o total para comparar depois. A tabela permanece entre reinicializações enquanto o volume do PostgreSQL for preservado.

## 4. Publicar um lote pequeno

Envie dez conexões fictícias:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/connections/mock?count=10"
```

Esperado: resposta HTTP `202` com `count` igual a `10`, um `batch_id` e `status` igual a `queued`. `queued` confirma publicação aceita pelo broker; não significa que a gravação no banco já terminou.

Para testar o limite máximo:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/connections/mock?count=200"
```

Esperado: HTTP `202`, `count` igual a `200`.

Para verificar limites, `count=0` e `count=201` devem ser rejeitados com HTTP `422` pela validação da API.

## 5. Verificar tópico, grupo e lag

O tópico é criado automaticamente pelo broker ao publicar a primeira mensagem. Consulte:

```powershell
docker compose exec redpanda rpk topic list
docker compose exec redpanda rpk topic describe connections.mock
docker compose exec redpanda rpk group list
docker compose exec redpanda rpk group describe connections-persistor
```

Esperado:

- `connections.mock` aparece na lista de tópicos.
- O grupo `connections-persistor` aparece após o worker iniciar o consumo.
- Após o processamento dos lotes, o lag desse grupo deve voltar a zero ou ficar próximo de zero.

As mensagens permanecem no tópico conforme a retenção configurada; o consumo não equivale a apagar a mensagem.

Para inspecionar o payload publicado, abra uma sessão de consumo em um terminal separado:

```powershell
docker compose exec redpanda rpk topic consume connections.mock
```

Interrompa com `Ctrl+C`. Uma sessão de inspeção pode ler mensagens sem representar a confirmação do grupo da aplicação; use o grupo `connections-persistor` para avaliar o progresso do worker.

## 6. Confirmar persistência

Consulte a quantidade e alguns campos não sensíveis:

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT count(*) AS total FROM ecommerce.connections;"
docker compose exec postgres psql -U airflow -d airflow -c "SELECT userid, login, date_access FROM ecommerce.connections ORDER BY date_access DESC LIMIT 10;"
```

Compare o total com o valor inicial. Após o lote de dez, o total deve aumentar em dez; após também publicar o lote de 200, deve aumentar em mais 200.

Verifique o formato do hash sem revelar seu conteúdo:

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT count(*) AS hashes_pbkdf2 FROM ecommerce.connections WHERE hashpass LIKE 'pbkdf2_sha256$%';"
```

Não consulte nem compartilhe `hashpass` em saída de terminal. Os dados são gerados para teste, não são credenciais reais.

## 7. Verificar logs e mensagens rejeitadas

```powershell
docker compose logs --tail=100 backend-api ecommerce-worker redpanda
```

Se o worker não conseguir persistir um lote por erro transitório de banco, ele não deve confirmar o offset e o Kafka poderá entregar a mensagem novamente. A persistência usa transação e conflitos de `userid`/`login` são ignorados para que reentregas não dupliquem registros.

Mensagens inválidas são encaminhadas para a tabela de dead letters existente:

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT id, reason, created_at FROM ecommerce.dead_letter_events ORDER BY id DESC LIMIT 10;"
```

## 8. Testes automatizados

A partir da pasta `backend`, execute:

```powershell
python -m unittest discover -s tests -v
```

Os testes do producer verificam geração de lote, unicidade dos IDs/logins, formato verificável do hash e rejeição de quantidade superior a 200. Os testes de schema existentes também devem continuar passando.

## Diagnóstico rápido

- **HTTP 503 em `/connections/mock`:** conferir se `redpanda` está saudável e se `backend-api` consegue alcançá-lo; consultar logs da API e do broker.
- **Resposta `queued`, mas contagem não aumenta:** verificar logs de `ecommerce-worker`, estado/lag do grupo `connections-persistor`, migration `0005_connections` e dead letters.
- **Grupo não aparece:** confirmar que o worker está em execução e que `CONNECTIONS_TOPIC`/`CONNECTIONS_GROUP_ID` estão configurados no serviço.
- **Falha de pull de imagem ou timeout TLS:** é problema de acesso ao registry, não uma falha da lógica da fila; repetir após a conectividade com Docker Hub voltar.

## Limpeza

Parar os serviços sem apagar os dados persistidos:

```powershell
docker compose down
```

Não use `docker compose down -v` para um teste que precise preservar os registros; esse comando apaga o volume do PostgreSQL e os dados do Redpanda.
