CREATE DATABASE IF NOT EXISTS efd_creditos;
USE efd_creditos;

-- =========================
-- EMPRESA
-- =========================
CREATE TABLE empresa (
    id INT AUTO_INCREMENT PRIMARY KEY,
    razao_social VARCHAR(255),
    cnpj VARCHAR(14) UNIQUE
);

-- =========================
-- ARQUIVO (UPLOAD)
-- =========================
CREATE TABLE efd_arquivo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nome_arquivo VARCHAR(255),
    periodo CHAR(6) NOT NULL, -- YYYYMM
    data_upload DATETIME DEFAULT CURRENT_TIMESTAMP,
    line_ending ENUM('LF','CRLF') NOT NULL DEFAULT 'LF',
    status ENUM('ORIGINAL','EM_REVISAO','CORRIGIDO') DEFAULT 'ORIGINAL',
    FOREIGN KEY (empresa_id) REFERENCES empresa(id),
    INDEX idx_empresa_periodo (empresa_id, periodo)
);

-- =========================
-- VERSÃO (SNAPSHOT/EDIÇÃO)
-- =========================
CREATE TABLE efd_versao (
    id INT AUTO_INCREMENT PRIMARY KEY,
    arquivo_id INT NOT NULL,
    numero INT NOT NULL,
    data_geracao DATETIME DEFAULT CURRENT_TIMESTAMP,
    observacao TEXT,

    -- necessário para seu workflow_service / versioning
    status ENUM('GERADA','EM_REVISAO','VALIDADA','EXPORTADA') DEFAULT 'GERADA',

    FOREIGN KEY (arquivo_id) REFERENCES efd_arquivo(id),
    UNIQUE (arquivo_id, numero),
    INDEX idx_arquivo_status (arquivo_id, status)
);

-- =========================
-- REGISTROS (LINHAS DO SPED)
-- =========================
CREATE TABLE efd_registro (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    versao_id INT NOT NULL,
    linha INT NOT NULL,
    reg CHAR(4) NOT NULL,
    conteudo_json JSON NOT NULL,
    alterado BOOLEAN DEFAULT FALSE,

    -- campos auxiliares (se você for usar consolidação por crédito)
    base_credito DECIMAL(15,2) DEFAULT 0,
    valor_credito DECIMAL(15,2) DEFAULT 0,
    tipo_credito VARCHAR(50),

    FOREIGN KEY (versao_id) REFERENCES efd_versao(id),
    INDEX idx_versao_linha (versao_id, linha),
    INDEX idx_reg (reg),
    INDEX idx_versao_reg (versao_id, reg)
);

-- =========================
-- APONTAMENTOS (ACHADOS DO SCANNER)
-- =========================
CREATE TABLE efd_apontamento (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- importante: facilita filtrar por versão sem depender só do join via registro
    versao_id INT NOT NULL,

    registro_id BIGINT NOT NULL,
    tipo ENUM('ERRO','OPORTUNIDADE') NOT NULL,
    codigo VARCHAR(30),
    descricao TEXT,
    impacto_financeiro DECIMAL(15,2),
    resolvido BOOLEAN DEFAULT FALSE,

    FOREIGN KEY (versao_id) REFERENCES efd_versao(id),
    FOREIGN KEY (registro_id) REFERENCES efd_registro(id),

    INDEX idx_versao_tipo_resolvido (versao_id, tipo, resolvido),
    INDEX idx_registro (registro_id)
);

-- =========================
-- TESES (CATÁLOGO)
-- =========================
CREATE TABLE tese_fiscal (
    id INT AUTO_INCREMENT PRIMARY KEY,
    codigo VARCHAR(30),
    descricao TEXT,
    fundamento_legal TEXT,
    risco ENUM('BAIXO','MEDIO','ALTO')
);

-- RELAÇÃO N:N (apontamento pode “citar” tese)
CREATE TABLE apontamento_tese (
    apontamento_id BIGINT,
    tese_id INT,
    PRIMARY KEY (apontamento_id, tese_id),
    FOREIGN KEY (apontamento_id) REFERENCES efd_apontamento(id),
    FOREIGN KEY (tese_id) REFERENCES tese_fiscal(id)
);

-- =========================
-- CRÉDITO APURADO (OPCIONAL)
-- =========================
CREATE TABLE credito_apurado (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    periodo CHAR(6),
    tipo VARCHAR(50),
    valor DECIMAL(15,2),
    data_calculo DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (empresa_id) REFERENCES empresa(id),
    INDEX idx_empresa_periodo (empresa_id, periodo)
);

# criacao das tabelas de cfops csts e tal

CREATE TABLE fiscal_grupo ( id INT AUTO_INCREMENT PRIMARY KEY,
slug VARCHAR(80) NOT NULL UNIQUE, descricao VARCHAR(255) NOT NULL,
tipo ENUM('CFOP', 'CST_PIS', 'CST_COFINS') NOT NULL, ativo BOOLEAN NOT NULL DEFAULT 1,
 ordem INT DEFAULT 0 );

 CREATE TABLE fiscal_grupo_item ( id INT AUTO_INCREMENT PRIMARY KEY,
 grupo_id INT NOT NULL, codigo VARCHAR(10) NOT NULL, descricao VARCHAR(255), ativo BOOLEAN NOT NULL DEFAULT 1,
  empresa_id INT NULL, peso INT DEFAULT 0, vig_ini DATE NULL, vig_fim DATE NULL, CONSTRAINT fk_fiscal_grupo
  FOREIGN KEY (grupo_id) REFERENCES fiscal_grupo(id), CONSTRAINT fk_fiscal_empresa
  FOREIGN KEY (empresa_id) REFERENCES empresa(id), UNIQUE KEY uq_grupo_codigo_empresa (grupo_id, codigo, empresa_id) );



START TRANSACTION;
SET SQL_SAFE_UPDATES = 0;

-- =========================================================
-- 0) LISTA DE SLUGS DO SEED (use em todos os deletes)
-- =========================================================
-- (MySQL não tem array, então repetimos o IN mesmo)

-- =========================================================
-- 1) LIMPA ITENS GLOBAIS DOS GRUPOS DO SEED
--    (não mexe nos itens por empresa)
-- =========================================================
DELETE i
FROM fiscal_grupo_item i
JOIN fiscal_grupo g ON g.id = i.grupo_id
WHERE i.empresa_id IS NULL
  AND g.slug IN (
    'CFOP_ENTRADA_REVENDA',
    'CFOP_SAIDA_REVENDA',
    'CFOP_ENTRADA_INDUSTRIALIZACAO',
    'CFOP_SAIDA_PRODUCAO',
    'CFOP_DEVOLUCAO_ENTRADA',
    'CFOP_DEVOLUCAO_SAIDA',
    'CFOP_TRANSFERENCIA_ENTRADA',
    'CFOP_TRANSFERENCIA_SAIDA',
    'CFOP_ST_SAIDA',
    'CFOP_CONSERTO_REMESSA',
    'CFOP_CONSERTO_RETORNO',
    'CFOP_BONIFICACAO_ENTRADA',
    'CFOP_BONIFICACAO_SAIDA',
    'CFOP_EXPORTACAO',
    'CST_PIS_RECEITA_TRIBUTADA',
    'CST_PIS_RECEITA_DIFERENCIADA',
    'CST_PIS_RECEITA_MONO_ST_OUTROS',
    'CST_PIS_ZERO_ISENTA_SUSP',
    'CST_PIS_CREDITO_NCUM',
    'CST_PIS_CREDITO_PRESUMIDO',
    'CST_PIS_AQUIS_SEM_CRED',
    'CST_PIS_OUTROS_ENTRADA',
    'CST_COFINS_RECEITA_TRIBUTADA',
    'CST_COFINS_RECEITA_DIFERENCIADA',
    'CST_COFINS_RECEITA_MONO_ST_OUTROS',
    'CST_COFINS_ZERO_ISENTA_SUSP',
    'CST_COFINS_CREDITO_NCUM',
    'CST_COFINS_CREDITO_PRESUMIDO',
    'CST_COFINS_AQUIS_SEM_CRED',
    'CST_COFINS_OUTROS_ENTRADA'
  );

-- =========================================================
-- 2) (Opcional) Se você quer apagar também os grupos do seed:
--    só apaga grupos que ficaram sem itens (seguro)
-- =========================================================
DELETE g
FROM fiscal_grupo g
LEFT JOIN fiscal_grupo_item i ON i.grupo_id = g.id
WHERE g.slug IN (
    'CFOP_ENTRADA_REVENDA',
    'CFOP_SAIDA_REVENDA',
    'CFOP_ENTRADA_INDUSTRIALIZACAO',
    'CFOP_SAIDA_PRODUCAO',
    'CFOP_DEVOLUCAO_ENTRADA',
    'CFOP_DEVOLUCAO_SAIDA',
    'CFOP_TRANSFERENCIA_ENTRADA',
    'CFOP_TRANSFERENCIA_SAIDA',
    'CFOP_ST_SAIDA',
    'CFOP_CONSERTO_REMESSA',
    'CFOP_CONSERTO_RETORNO',
    'CFOP_BONIFICACAO_ENTRADA',
    'CFOP_BONIFICACAO_SAIDA',
    'CFOP_EXPORTACAO',
    'CST_PIS_RECEITA_TRIBUTADA',
    'CST_PIS_RECEITA_DIFERENCIADA',
    'CST_PIS_RECEITA_MONO_ST_OUTROS',
    'CST_PIS_ZERO_ISENTA_SUSP',
    'CST_PIS_CREDITO_NCUM',
    'CST_PIS_CREDITO_PRESUMIDO',
    'CST_PIS_AQUIS_SEM_CRED',
    'CST_PIS_OUTROS_ENTRADA',
    'CST_COFINS_RECEITA_TRIBUTADA',
    'CST_COFINS_RECEITA_DIFERENCIADA',
    'CST_COFINS_RECEITA_MONO_ST_OUTROS',
    'CST_COFINS_ZERO_ISENTA_SUSP',
    'CST_COFINS_CREDITO_NCUM',
    'CST_COFINS_CREDITO_PRESUMIDO',
    'CST_COFINS_AQUIS_SEM_CRED',
    'CST_COFINS_OUTROS_ENTRADA'
)
AND i.id IS NULL;

-- =========================================================
-- 3) RECRIA GRUPOS (UPSERT)
-- =========================================================
INSERT INTO fiscal_grupo (slug, descricao, tipo, ativo, ordem) VALUES
('CFOP_ENTRADA_REVENDA',            'Entradas para comercialização/revenda',                         'CFOP', 1, 10),
('CFOP_SAIDA_REVENDA',              'Saídas de mercadoria adquirida/recebida de terceiros (revenda)', 'CFOP', 1, 20),
('CFOP_ENTRADA_INDUSTRIALIZACAO',   'Entradas para industrialização',                                'CFOP', 1, 30),
('CFOP_SAIDA_PRODUCAO',             'Saídas de produção do estabelecimento',                         'CFOP', 1, 40),
('CFOP_DEVOLUCAO_ENTRADA',          'Devoluções relacionadas a vendas/saídas (entrada)',             'CFOP', 1, 50),
('CFOP_DEVOLUCAO_SAIDA',            'Devoluções relacionadas a compras/entradas (saída)',            'CFOP', 1, 60),
('CFOP_TRANSFERENCIA_ENTRADA',      'Transferência - entradas',                                      'CFOP', 1, 70),
('CFOP_TRANSFERENCIA_SAIDA',        'Transferência - saídas',                                        'CFOP', 1, 80),
('CFOP_ST_SAIDA',                   'Saídas com Substituição Tributária (ST)',                       'CFOP', 1, 90),
('CFOP_CONSERTO_REMESSA',           'Remessa para conserto/reparo',                                  'CFOP', 1, 100),
('CFOP_CONSERTO_RETORNO',           'Retorno de conserto/reparo',                                    'CFOP', 1, 110),
('CFOP_BONIFICACAO_ENTRADA',        'Entrada de bonificação/doação/brinde',                          'CFOP', 1, 120),
('CFOP_BONIFICACAO_SAIDA',          'Saída em bonificação/doação/brinde',                            'CFOP', 1, 130),
('CFOP_EXPORTACAO',                 'Exportação (família 7xxx)',                                     'CFOP', 1, 140),

('CST_PIS_RECEITA_TRIBUTADA',       'CST PIS: receita tributada (01/02)',                            'CST_PIS', 1, 10),
('CST_PIS_RECEITA_DIFERENCIADA',    'CST PIS: receita diferenciada/unidade (03)',                    'CST_PIS', 1, 20),
('CST_PIS_RECEITA_MONO_ST_OUTROS',  'CST PIS: monofásica/ST/outras saídas (04/05/49)',               'CST_PIS', 1, 30),
('CST_PIS_ZERO_ISENTA_SUSP',        'CST PIS: alíquota zero/isenta/sem incidência/suspensão (06-09)','CST_PIS', 1, 40),
('CST_PIS_CREDITO_NCUM',            'CST PIS: crédito não-cumulativo (50-56)',                       'CST_PIS', 1, 50),
('CST_PIS_CREDITO_PRESUMIDO',       'CST PIS: crédito presumido (60-67)',                            'CST_PIS', 1, 60),
('CST_PIS_AQUIS_SEM_CRED',          'CST PIS: aquisições sem crédito (70-75)',                       'CST_PIS', 1, 70),
('CST_PIS_OUTROS_ENTRADA',          'CST PIS: outras entradas (98/99)',                              'CST_PIS', 1, 80),

('CST_COFINS_RECEITA_TRIBUTADA',      'CST COFINS: receita tributada (01/02)',                       'CST_COFINS', 1, 10),
('CST_COFINS_RECEITA_DIFERENCIADA',   'CST COFINS: receita diferenciada/unidade (03)',               'CST_COFINS', 1, 20),
('CST_COFINS_RECEITA_MONO_ST_OUTROS', 'CST COFINS: monofásica/ST/outras saídas (04/05/49)',          'CST_COFINS', 1, 30),
('CST_COFINS_ZERO_ISENTA_SUSP',       'CST COFINS: alíquota zero/isenta/sem incidência/suspensão (06-09)','CST_COFINS', 1, 40),
('CST_COFINS_CREDITO_NCUM',           'CST COFINS: crédito não-cumulativo (50-56)',                  'CST_COFINS', 1, 50),
('CST_COFINS_CREDITO_PRESUMIDO',      'CST COFINS: crédito presumido (60-67)',                       'CST_COFINS', 1, 60),
('CST_COFINS_AQUIS_SEM_CRED',         'CST COFINS: aquisições sem crédito (70-75)',                  'CST_COFINS', 1, 70),
('CST_COFINS_OUTROS_ENTRADA',         'CST COFINS: outras entradas (98/99)',                         'CST_COFINS', 1, 80)
ON DUPLICATE KEY UPDATE
  descricao=VALUES(descricao),
  tipo=VALUES(tipo),
  ativo=VALUES(ativo),
  ordem=VALUES(ordem);

-- =========================================================
-- 4) REINSERE ITENS (FORMATO "À PROVA DE ALIAS")
-- =========================================================

-- CFOP_SAIDA_REVENDA
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5102', 'Venda de mercadoria de terceiros (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_SAIDA_REVENDA'
UNION ALL
SELECT g.id, '6102', 'Venda de mercadoria de terceiros (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_SAIDA_REVENDA';

-- CFOP_ENTRADA_REVENDA
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '1102', 'Compra para comercialização (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_ENTRADA_REVENDA'
UNION ALL
SELECT g.id, '2102', 'Compra para comercialização (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_ENTRADA_REVENDA'
UNION ALL
SELECT g.id, '3102', 'Compra para comercialização (exterior/importação)', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_ENTRADA_REVENDA';

-- CFOP_ENTRADA_INDUSTRIALIZACAO
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '1101', 'Compra para industrialização (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_ENTRADA_INDUSTRIALIZACAO'
UNION ALL
SELECT g.id, '2101', 'Compra para industrialização (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_ENTRADA_INDUSTRIALIZACAO'
UNION ALL
SELECT g.id, '3101', 'Compra para industrialização (exterior/importação)', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_ENTRADA_INDUSTRIALIZACAO';

-- CFOP_SAIDA_PRODUCAO
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5101', 'Venda produção do estabelecimento (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_SAIDA_PRODUCAO'
UNION ALL
SELECT g.id, '6101', 'Venda produção do estabelecimento (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_SAIDA_PRODUCAO';

-- CFOP_DEVOLUCAO_ENTRADA
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '1202', 'Devolução de venda (intra) - mercadoria de terceiros', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_ENTRADA'
UNION ALL
SELECT g.id, '2202', 'Devolução de venda (inter) - mercadoria de terceiros', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_ENTRADA'
UNION ALL
SELECT g.id, '1201', 'Devolução de venda (intra) - produção do estabelecimento', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_ENTRADA'
UNION ALL
SELECT g.id, '2201', 'Devolução de venda (inter) - produção do estabelecimento', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_ENTRADA';

-- CFOP_DEVOLUCAO_SAIDA
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5202', 'Devolução de compra (intra) - mercadoria para revenda', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_SAIDA'
UNION ALL
SELECT g.id, '6202', 'Devolução de compra (inter) - mercadoria para revenda', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_SAIDA'
UNION ALL
SELECT g.id, '5201', 'Devolução de compra (intra) - insumo/industrialização', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_SAIDA'
UNION ALL
SELECT g.id, '6201', 'Devolução de compra (inter) - insumo/industrialização', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_DEVOLUCAO_SAIDA';

-- CFOP_TRANSFERENCIA_ENTRADA
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '1152', 'Transferência entrada mercadoria de terceiros (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_ENTRADA'
UNION ALL
SELECT g.id, '2152', 'Transferência entrada mercadoria de terceiros (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_ENTRADA'
UNION ALL
SELECT g.id, '1552', 'Transferência entrada produção do estabelecimento (intra)', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_ENTRADA'
UNION ALL
SELECT g.id, '2552', 'Transferência entrada produção do estabelecimento (inter)', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_ENTRADA';

-- CFOP_TRANSFERENCIA_SAIDA
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5152', 'Transferência saída mercadoria de terceiros (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_SAIDA'
UNION ALL
SELECT g.id, '6152', 'Transferência saída mercadoria de terceiros (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_SAIDA'
UNION ALL
SELECT g.id, '5552', 'Transferência saída produção do estabelecimento (intra)', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_SAIDA'
UNION ALL
SELECT g.id, '6552', 'Transferência saída produção do estabelecimento (inter)', 1, NULL, 5
FROM fiscal_grupo g WHERE g.slug='CFOP_TRANSFERENCIA_SAIDA';

-- CFOP_ST_SAIDA
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5401', 'Venda produção com ST (substituto)', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CFOP_ST_SAIDA'
UNION ALL
SELECT g.id, '5403', 'Venda mercadoria de terceiros com ST (substituto)', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CFOP_ST_SAIDA'
UNION ALL
SELECT g.id, '5405', 'Venda mercadoria de terceiros com ST (substituído)', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CFOP_ST_SAIDA'
UNION ALL
SELECT g.id, '6401', 'Venda produção com ST (inter/fora UF)', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CFOP_ST_SAIDA'
UNION ALL
SELECT g.id, '6403', 'Venda mercadoria de terceiros com ST (inter/fora UF)', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CFOP_ST_SAIDA'
UNION ALL
SELECT g.id, '6404', 'Venda mercadoria com ST retida anteriormente', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CFOP_ST_SAIDA';

-- CFOP_CONSERTO_REMESSA e RETORNO
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5915', 'Remessa para conserto/reparo (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_CONSERTO_REMESSA'
UNION ALL
SELECT g.id, '6915', 'Remessa para conserto/reparo (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_CONSERTO_REMESSA';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5916', 'Retorno de conserto/reparo (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_CONSERTO_RETORNO'
UNION ALL
SELECT g.id, '6916', 'Retorno de conserto/reparo (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_CONSERTO_RETORNO';

-- CFOP_BONIFICACAO
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '1910', 'Entrada de bonificação/doação/brinde', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_BONIFICACAO_ENTRADA';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '5910', 'Saída em bonificação/doação/brinde (intra)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_BONIFICACAO_SAIDA'
UNION ALL
SELECT g.id, '6910', 'Saída em bonificação/doação/brinde (inter)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_BONIFICACAO_SAIDA';

-- CFOP_EXPORTACAO: prefixo
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '7*', 'Qualquer CFOP 7xxx (exportação)', 1, NULL, 10
FROM fiscal_grupo g WHERE g.slug='CFOP_EXPORTACAO';

-- ============================
-- CST PIS (01..99 - completos)
-- ============================

-- Receita tributada (01/02)
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '01', 'Tributável - Alíquota Básica', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_RECEITA_TRIBUTADA'
UNION ALL
SELECT g.id, '02', 'Tributável - Alíquota Diferenciada', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_RECEITA_TRIBUTADA';

-- Diferenciada/unidade (03)
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '03', 'Tributável - Alíquota por Unidade', 1, NULL, 8
FROM fiscal_grupo g WHERE g.slug='CST_PIS_RECEITA_DIFERENCIADA';

-- Mono/ST/outros (04/05/49)
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '04', 'Monofásica - Revenda a Alíquota Zero', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CST_PIS_RECEITA_MONO_ST_OUTROS'
UNION ALL
SELECT g.id, '05', 'Substituição Tributária', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CST_PIS_RECEITA_MONO_ST_OUTROS'
UNION ALL
SELECT g.id, '49', 'Outras Operações de Saída', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CST_PIS_RECEITA_MONO_ST_OUTROS';

-- Zero/isenta/susp (06-09)
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '06', 'Alíquota Zero', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_ZERO_ISENTA_SUSP'
UNION ALL SELECT g.id, '07', 'Isenta', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_ZERO_ISENTA_SUSP'
UNION ALL SELECT g.id, '08', 'Sem Incidência', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_ZERO_ISENTA_SUSP'
UNION ALL SELECT g.id, '09', 'Suspensão', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_ZERO_ISENTA_SUSP';

-- Crédito NCUM 50-56
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '50', 'Crédito NCUM - Tributada MI', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_NCUM'
UNION ALL SELECT g.id, '51', 'Crédito NCUM - Não Tributada MI', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_NCUM'
UNION ALL SELECT g.id, '52', 'Crédito NCUM - Exportação', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_NCUM'
UNION ALL SELECT g.id, '53', 'Crédito NCUM - Misto MI', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_NCUM'
UNION ALL SELECT g.id, '54', 'Crédito NCUM - Tributada MI e Exportação', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_NCUM'
UNION ALL SELECT g.id, '55', 'Crédito NCUM - Não Tributada MI e Exportação', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_NCUM'
UNION ALL SELECT g.id, '56', 'Crédito NCUM - Misto (MI+EXP)', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_NCUM';

-- Crédito Presumido 60-67
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '60', 'Crédito Presumido - Tributada MI', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '61', 'Crédito Presumido - Não Tributada MI', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '62', 'Crédito Presumido - Exportação', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '63', 'Crédito Presumido - Misto MI', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '64', 'Crédito Presumido - Tributada MI e Exportação', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '65', 'Crédito Presumido - Não Tributada MI e Exportação', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '66', 'Crédito Presumido - Misto (MI+EXP)', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '67', 'Crédito Presumido - Outras Operações', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_CREDITO_PRESUMIDO';

-- Aquisição sem crédito 70-75
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '70', 'Aquisição sem direito a crédito', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '71', 'Aquisição com Isenção', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '72', 'Aquisição com Suspensão', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '73', 'Aquisição a Alíquota Zero', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '74', 'Aquisição sem Incidência', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '75', 'Aquisição por Substituição Tributária', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_PIS_AQUIS_SEM_CRED';

-- Outros 98/99
INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '98', 'Outras Operações de Entrada', 1, NULL, 5 FROM fiscal_grupo g WHERE g.slug='CST_PIS_OUTROS_ENTRADA'
UNION ALL
SELECT g.id, '99', 'Outras Operações', 1, NULL, 5 FROM fiscal_grupo g WHERE g.slug='CST_PIS_OUTROS_ENTRADA';

-- ============================
-- CST COFINS (mesmo espelho)
-- ============================

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '01', 'Tributável - Alíquota Básica', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_RECEITA_TRIBUTADA'
UNION ALL
SELECT g.id, '02', 'Tributável - Alíquota Diferenciada', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_RECEITA_TRIBUTADA';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '03', 'Tributável - Alíquota por Unidade', 1, NULL, 8
FROM fiscal_grupo g WHERE g.slug='CST_COFINS_RECEITA_DIFERENCIADA';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '04', 'Monofásica - Revenda a Alíquota Zero', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_RECEITA_MONO_ST_OUTROS'
UNION ALL
SELECT g.id, '05', 'Substituição Tributária', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_RECEITA_MONO_ST_OUTROS'
UNION ALL
SELECT g.id, '49', 'Outras Operações de Saída', 1, NULL, 8 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_RECEITA_MONO_ST_OUTROS';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '06', 'Alíquota Zero', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_ZERO_ISENTA_SUSP'
UNION ALL SELECT g.id, '07', 'Isenta', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_ZERO_ISENTA_SUSP'
UNION ALL SELECT g.id, '08', 'Sem Incidência', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_ZERO_ISENTA_SUSP'
UNION ALL SELECT g.id, '09', 'Suspensão', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_ZERO_ISENTA_SUSP';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '50', 'Crédito NCUM - Tributada MI', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_NCUM'
UNION ALL SELECT g.id, '51', 'Crédito NCUM - Não Tributada MI', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_NCUM'
UNION ALL SELECT g.id, '52', 'Crédito NCUM - Exportação', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_NCUM'
UNION ALL SELECT g.id, '53', 'Crédito NCUM - Misto MI', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_NCUM'
UNION ALL SELECT g.id, '54', 'Crédito NCUM - Tributada MI e Exportação', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_NCUM'
UNION ALL SELECT g.id, '55', 'Crédito NCUM - Não Tributada MI e Exportação', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_NCUM'
UNION ALL SELECT g.id, '56', 'Crédito NCUM - Misto (MI+EXP)', 1, NULL, 10 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_NCUM';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '60', 'Crédito Presumido - Tributada MI', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '61', 'Crédito Presumido - Não Tributada MI', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '62', 'Crédito Presumido - Exportação', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '63', 'Crédito Presumido - Misto MI', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '64', 'Crédito Presumido - Tributada MI e Exportação', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '65', 'Crédito Presumido - Não Tributada MI e Exportação', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '66', 'Crédito Presumido - Misto (MI+EXP)', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO'
UNION ALL SELECT g.id, '67', 'Crédito Presumido - Outras Operações', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_CREDITO_PRESUMIDO';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '70', 'Aquisição sem direito a crédito', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '71', 'Aquisição com Isenção', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '72', 'Aquisição com Suspensão', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '73', 'Aquisição a Alíquota Zero', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '74', 'Aquisição sem Incidência', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_AQUIS_SEM_CRED'
UNION ALL SELECT g.id, '75', 'Aquisição por Substituição Tributária', 1, NULL, 6 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_AQUIS_SEM_CRED';

INSERT INTO fiscal_grupo_item (grupo_id, codigo, descricao, ativo, empresa_id, peso)
SELECT g.id, '98', 'Outras Operações de Entrada', 1, NULL, 5 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_OUTROS_ENTRADA'
UNION ALL
SELECT g.id, '99', 'Outras Operações', 1, NULL, 5 FROM fiscal_grupo g WHERE g.slug='CST_COFINS_OUTROS_ENTRADA';
SET SQL_SAFE_UPDATES = 1;

COMMIT;
