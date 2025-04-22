with source as (
    select * from {{ source ('amazon', 'produtos_amazon')}}
),

renamed as (
    select 
        {{ adapter.quote("asin") }} as id_produto,
        {{ adapter.quote("currency")}} as moeda,
        {{ adapter.quote("product_minimum_offer_price")}} as oferta_minima_produto,
        {{ adapter.quote("product_num_offers")}} as num_ofertas_produto,
        {{ adapter.quote("product_num_ratings" )}} as num_avaliacoes_produto,
        {{ adapter.quote("product_original_price") }} as preco_original_produto,
        {{ adapter.quote("product_photo")}} as foto_produto_url,
        {{ adapter.quote("product_price")}} as preco_produto,
        {{ adapter.quote("product_star_rating") }} as estrelas_avaliacao_produto,
        {{ adapter.quote("product_url") }} as url_produto,
        {{ adapter.quote("sales_volume") }} as volume_vendas, 
        {{ adapter.quote("product_minimum_offers") }} as ofertas_minimas_produto,
        {{ adapter.quote("country") }} as pais
    from source
)

select *
from renamed