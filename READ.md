# dw-adventureworks

Este projeto consiste na implementação de um Data Warehouse (DW) utilizando a base de dados de exemplo **AdventureWorks**. O objetivo é demonstrar processos de ETL (Extract, Transform, Load), modelagem dimensional e análise de dados.

## 📋 Sobre o Projeto

O **dw-adventureworks** visa transformar o banco de dados transacional (OLTP) da AdventureWorks em um modelo dimensional (OLAP), facilitando a criação de relatórios e dashboards para tomada de decisão.

### Funcionalidades

-   **Modelagem Dimensional**: Criação de esquemas Estrela (Star Schema) ou Floco de Neve (Snowflake).
-   **ETL**: Scripts e pacotes para extração, limpeza e carga de dados.
-   **Análise**: Consultas analíticas e visualizações.

## 🛠️ Tecnologias Utilizadas

-   **Banco de Dados**: SQL Server
-   **ETL**: SQL Server Integration Services (SSIS) / T-SQL
-   **Modelagem**: SQL Server Management Studio (SSMS)
-   **Visualização**: Power BI / Excel

## 🚀 Como Executar

1.  **Pré-requisitos**:
    -   Instância do SQL Server instalada.
    -   Banco de dados `AdventureWorks` restaurado.

2.  **Instalação**:
    -   Clone este repositório:
        ```bash
        git clone https://github.com/seu-usuario/dw-adventureworks.git
        ```
    -   Execute os scripts SQL localizados na pasta `/scripts` para criar as tabelas de dimensão e fato.

