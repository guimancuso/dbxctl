# dbxctl - Manual do Usuario

**dbxctl** e uma ferramenta de linha de comando para gerenciamento idempotente de usuarios, grupos, memberships e atribuicoes de workspace no Databricks Account.

---

## Sumario

1. [Visao Geral](#visao-geral)
2. [Pre-requisitos](#pre-requisitos)
3. [Instalacao](#instalacao)
4. [Configuracao](#configuracao)
   - [settings.yaml](#settingsyaml)
   - [users.yaml](#usersyaml)
   - [groups.yaml](#groupsyaml)
   - [Arquivos de Membership](#arquivos-de-membership)
   - [Arquivos de Workspace Assignment](#arquivos-de-workspace-assignment)
5. [Comandos](#comandos)
   - [validate](#validate)
   - [users](#users)
   - [groups](#groups)
   - [members](#members)
   - [workspaces](#workspaces)
   - [sync](#sync)
6. [Modo Dry-Run](#modo-dry-run)
7. [Mecanismo de Protecao](#mecanismo-de-protecao)
8. [Exemplos](#exemplos)

---

## Visao Geral

O dbxctl le arquivos de configuracao YAML e sincroniza o estado desejado com sua conta Databricks via API SCIM (Databricks SDK). Ele e **idempotente** — executar multiplas vezes produz o mesmo resultado sem efeitos colaterais.

A ordem de sincronizacao e:
1. Criar grupos
2. Criar usuarios
3. Reconciliar memberships (adicionar/remover membros)
4. Atribuir grupos aos workspaces

Recursos que nao estao presentes nos arquivos YAML sao removidos (a menos que estejam protegidos).

## Pre-requisitos

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Uma conta Databricks com acesso de administrador
- Perfis do Databricks CLI configurados em `~/.databrickscfg`

## Instalacao

```bash
# Clone o repositorio
git clone <url-do-repositorio>
cd dbxctl

# Instalar dependencias e o projeto
uv sync
```

Apos a instalacao, o comando `dbxctl` estara disponivel no seu PATH.

## Configuracao

Todos os arquivos de configuracao ficam no diretorio `config/`.

### settings.yaml

Arquivo de configuracao principal com credenciais da conta, definicoes de workspace e listas de protecao.

```yaml
account:
  host: "https://accounts.cloud.databricks.com"
  account_id: "seu-account-id"
  profile: "seu-perfil-databricks-cli"

workspaces:
  - name: "production"
    host: "https://dbc-xxxxx.cloud.databricks.com"
    profile: "ws-prd-perfil"

  - name: "development"
    host: "https://dbc-yyyyy.cloud.databricks.com"
    profile: "ws-dev-perfil"

# Emails e grupos protegidos contra exclusao
protected_emails:
  - "admin@empresa.com"

protected_groups:
  - "GRP-ALL-WORKSPACE-ADMIN"
```

| Campo | Obrigatorio | Descricao |
|-------|-------------|-----------|
| `account.host` | Sim | URL do console de contas do Databricks |
| `account.account_id` | Sim | ID da sua conta Databricks |
| `account.profile` | Sim | Nome do perfil Databricks CLI para operacoes em nivel de conta |
| `workspaces[].name` | Sim | Nome logico do workspace (deve corresponder ao nome do arquivo YAML) |
| `workspaces[].host` | Sim | URL do workspace |
| `workspaces[].profile` | Sim | Perfil Databricks CLI para operacoes no workspace |
| `protected_emails` | Nao | Lista de emails que nunca serao excluidos |
| `protected_groups` | Nao | Lista de nomes de grupos que nunca serao excluidos |

### users.yaml

Define todos os usuarios a serem gerenciados na conta Databricks.

```yaml
users:
  - email: joao.silva@empresa.com
    display_name: Joao Silva
  - email: maria.santos@empresa.com
    display_name: Maria Santos
```

| Campo | Obrigatorio | Descricao |
|-------|-------------|-----------|
| `email` | Sim | Email do usuario (deve ser um formato valido) |
| `display_name` | Sim | Nome de exibicao do usuario no Databricks |

### groups.yaml

Define todos os grupos a serem gerenciados.

```yaml
groups:
  - name: GRP-ENGENHARIA-DADOS
    description: "Time de Engenharia de Dados"
  - name: GRP-ANALISTAS-DADOS
    description: "Time de Analise de Dados"
  - name: SVC-AUTOMACAO
    description: "Service principals para automacao"
```

| Campo | Obrigatorio | Descricao |
|-------|-------------|-----------|
| `name` | Sim | Nome do grupo (apenas letras, numeros, pontos, hifens e underscores) |
| `description` | Nao | Descricao do grupo |

### Arquivos de Membership

Localizados em `config/memberships/`. Cada arquivo e nomeado com o nome do grupo (ex: `GRP-ENGENHARIA-DADOS.yaml`) e contem a lista de emails dos membros.

```yaml
# config/memberships/GRP-ENGENHARIA-DADOS.yaml
- joao.silva@empresa.com
- maria.santos@empresa.com
```

O nome do grupo e derivado do nome do arquivo (sem a extensao `.yaml`).

### Arquivos de Workspace Assignment

Localizados em `config/workspaces/`. Cada arquivo e nomeado com o nome do workspace (ex: `production.yaml`) e define quais grupos tem acesso e com qual nivel de permissao.

```yaml
# config/workspaces/production.yaml
- group: GRP-ENGENHARIA-DADOS
  permission: USER

- group: GRP-ALL-WORKSPACE-ADMIN
  permission: ADMIN
```

| Campo | Obrigatorio | Descricao |
|-------|-------------|-----------|
| `group` | Sim | Nome do grupo (deve existir em groups.yaml) |
| `permission` | Sim | `USER` ou `ADMIN` |

## Comandos

### validate

Valida todos os arquivos de configuracao YAML sem fazer nenhuma chamada de API. Realiza verificacoes de referencia cruzada.

```bash
dbxctl validate
```

Verificacoes realizadas:
- Existencia dos arquivos e sintaxe YAML
- Campos obrigatorios presentes
- Validacao de formato de email
- Deteccao de duplicatas (emails, grupos, membros)
- Referencia cruzada: emails de membership existem em users.yaml
- Referencia cruzada: grupos de membership existem em groups.yaml
- Referencia cruzada: grupos de workspace existem em groups.yaml
- Referencia cruzada: nomes de workspace correspondem a settings.yaml

### users

Sincroniza usuarios com a conta Databricks.

```bash
dbxctl users [--dry-run]
```

- Cria usuarios presentes no YAML mas ausentes no Databricks
- Exclui usuarios presentes no Databricks mas ausentes no YAML (a menos que protegidos)

### groups

Sincroniza grupos com a conta Databricks.

```bash
dbxctl groups [--dry-run]
```

- Cria grupos presentes no YAML mas ausentes no Databricks
- Exclui grupos presentes no Databricks mas ausentes no YAML (a menos que protegidos ou grupos de sistema)

### members

Sincroniza memberships dos grupos.

```bash
dbxctl members [--dry-run]
```

- Adiciona usuarios aos grupos conforme definido nos arquivos de membership
- Remove usuarios dos grupos se nao estiverem no arquivo de membership

### workspaces

Sincroniza atribuicoes de grupos aos workspaces.

```bash
dbxctl workspaces [--dry-run]
```

- Atribui grupos aos workspaces com o nivel de permissao especificado
- Remove atribuicoes de grupos nao presentes no YAML

### sync

Executa todas as etapas de sincronizacao na ordem correta.

```bash
dbxctl sync [--dry-run]
```

Equivalente a executar: `groups` -> `users` -> `members` -> `workspaces`

## Modo Dry-Run

Todos os comandos que modificam dados suportam a flag `--dry-run`. Quando habilitada, o dbxctl mostra quais acoes **seriam** realizadas sem realmente executa-las.

```bash
dbxctl sync --dry-run
```

Labels de saida no modo dry-run:
- `DRY-RUN Would create: ...` (criaria)
- `DRY-RUN Would delete: ...` (excluiria)
- `DRY-RUN Would add: ...` (adicionaria)
- `DRY-RUN Would remove: ...` (removeria)

## Mecanismo de Protecao

O dbxctl possui multiplas camadas de protecao para evitar exclusoes acidentais:

1. **Entidades de sistema** — Grupos nativos do Databricks (`admins`, `users`, `account users`) e padroes de email de sistema (`databricks`, `serviceprincipals`) sao sempre protegidos.

2. **Protecao explicita** — Emails e grupos listados em `protected_emails` e `protected_groups` no `settings.yaml` nunca sao excluidos.

3. **Validacao** — O comando `validate` detecta erros de configuracao antes de qualquer chamada de API.

4. **Dry-run** — Visualize todas as alteracoes antes de aplica-las.

## Exemplos

### Fluxo completo

```bash
# 1. Validar configuracao
dbxctl validate

# 2. Visualizar alteracoes
dbxctl sync --dry-run

# 3. Aplicar alteracoes
dbxctl sync
```

### Sincronizar apenas usuarios

```bash
dbxctl users --dry-run
dbxctl users
```

### Modo verbose

```bash
dbxctl -v sync --dry-run
```

### Estrutura de diretorios

```
dbxctl/
├── config/
│   ├── settings.yaml
│   ├── users.yaml
│   ├── groups.yaml
│   ├── memberships/
│   │   ├── GRP-ENGENHARIA-DADOS.yaml
│   │   └── GRP-ANALISTAS-DADOS.yaml
│   └── workspaces/
│       ├── production.yaml
│       └── development.yaml
├── dbx_iam/
│   ├── client.py
│   ├── config_loader.py
│   ├── manage_groups.py
│   ├── manage_memberships.py
│   ├── manage_users.py
│   ├── manage_workspaces.py
│   └── models.py
├── dbxctl.py
└── pyproject.toml
```
