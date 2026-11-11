# AgroTrac Audit — Serviço Audit C (Histórico, Auditoria e Romaneios)

Microsserviço responsável pelo armazenamento imutável, histórico auditável de operações e ingestão de pesagens e romaneios do sistema **AgroTrac**, em estrita conformidade com a especificação original (`acac36b6`).

---

## 1. Responsabilidades

- **Consumo 100% Orientado a Eventos**: Ingestão assíncrona de eventos emitidos pelo Core A (reservas, cancelamentos, janelas) e Live B (sessões) através do RabbitMQ, sem leitura direta de tabelas de outros microsserviços.
- **Ingestão de Pesagens Offline**: Endpoint HTTP para recebimento do lote de pesagens geradas pelo aplicativo móvel no campo.
- **Armazenamento de Romaneios**: Recepção e guarda segura dos arquivos de fotos de romaneios enviados pelo app após restabelecimento de conexão.
- **Garantia de Idempotência**: Prevenção de duplicatas em caso de reenvios pelo app através de identificador único estável (`local_id` UUID).
- **Histórico Imutável**: Armazenamento cronológico append-only (sem UPDATE/DELETE operacional), garantindo que auditorias e consultas históricas não concorram nem deixem lentas as tabelas transacionais do Core A.
- **Relatórios Consolidados**: Consultas de conferência fiscal e análise de dados de safras passadas.

---

## 2. Ferramentas e Tecnologias

- **Runtime**: Node.js com TypeScript.
- **Banco de Dados**: PostgreSQL (instância/database `agrotrac_audit`, isolado do Core A com permissões append-only).
- **Acesso a Dados**: Knex.js ou Driver `pg`.
- **Upload e Arquivos**: `multer` para upload `multipart/form-data` e persistência em sistema de arquivos local (`/uploads`).
- **Broker de Mensageria**: `amqplib` (consumidor RabbitMQ com ACK manual após persistência).
- **Framework Web / HTTP**: Fastify ou Express.
- **Testes Automatizados**: Vitest ou Jest com Repositórios em Memória.

---

## 3. Arquitetura (Clean Architecture)

A estrutura do microsserviço isola as regras analíticas da infraestrutura:

```text
agrotrac-audit/
├── src/
│   ├── core/                    # Regras puras e Casos de Uso
│   │   ├── domain/              # Entidades e Erros de Auditoria
│   │   │   ├── entities/        # AuditLog, WeighingRecord, RomaneioAttachment
│   │   │   ├── errors/          # DuplicateRecordError, InvalidAttachmentError
│   │   │   └── repositories/    # Interfaces (IAuditRepository, IWeighingRepository)
│   │   └── use-cases/           # Casos de uso
│   │       ├── IngestEventUseCase.ts
│   │       ├── SyncWeighingUseCase.ts
│   │       ├── ListWeighingsUseCase.ts
│   │       └── GenerateHarvestReportUseCase.ts
│   ├── infra/                   # Implementações de infraestrutura
│   │   ├── database/            # Conexão PostgreSQL (agrotrac_audit) e Migrations
│   │   ├── messaging/           # Consumidor RabbitMQ da fila 'agrotrac.audit.events'
│   │   ├── storage/             # Adaptador de persistência de arquivos em disco local
│   │   └── http/                # Controllers e rotas de sincronização e relatórios
│   └── main.ts                  # Inicialização do serviço e consumidor AMQP
├── tests/
│   ├── in-memory/               # Repositórios em memória para testes isolados
│   └── unit/                    # Testes de unidade dos casos de uso
├── package.json
└── tsconfig.json
```

---

## 4. Conexões e Integrações

- **Entrada (HTTP via Gateway)**:
  - `POST /v1/audit/weighings/sync`: Endpoint que recebe `multipart/form-data` com os dados da pesagem e a imagem do romaneio enviada pelo mobile.
  - `GET /v1/audit/weighings`: Consulta pesagens sincronizadas para o Portal Web.
  - `GET /v1/audit/reports/fiscal`: Relatório consolidado para conferência fiscal.
- **Entrada (Mensageria RabbitMQ)**:
  - Fila: `agrotrac.audit.events` (ligada à exchange `agrotrac.events` ouvindo tópicos `agrotrac.core.*` e `agrotrac.live.*`).
- **Saída (Banco de Dados)**:
  - Conexão com PostgreSQL (`agrotrac_audit`) na porta `5432`.
- **Saída (Armazenamento de Imagens)**:
  - Diretório local `/uploads/romaneios` montado via volume Docker.
