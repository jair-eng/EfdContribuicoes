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
