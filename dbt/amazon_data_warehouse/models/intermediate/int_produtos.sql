with source as (
    select 
        id_produto,
        case
            when moeda is null and pais = "BR" then "BRL"
            when moeda is null and pais = "US" then "USD"
            else moeda
        end as moeda,   
        case 
            when oferta_minima_produto is null then 0
            else round(oferta_minima_produto, 2)
        end as ofertas_minima_produto,
        num_ofertas_produto,
        num_avaliacoes_produto,
        preco_original_produto,
        case
            when preco_produto is null then 0
            else round(preco_produto, 2)
        end as preco_produto,
        case
            when preco_original_produto = 0 or preco_produto = 0 then preco_original_produto
            else round(preco_original_produto - preco_produto, 2)
        end as diferenca_valor,
        titulo_produto,
        foto_produto_url,
        estrelas_avaliacao_produto,
        url_produto,
        volume_vendas, 
        pais
    from {{ref("stg_produtos")}}
)

select *
from source