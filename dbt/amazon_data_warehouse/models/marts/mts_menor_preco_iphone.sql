with source as (
    select min(preco_produto) as preco_minimo, left(titulo_produto, 15) as iphone
    from {{ref("int_produtos")}}
    group by left(titulo_produto, 15)

)

select *
from source