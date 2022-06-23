-- Удалите внешний ключ из sales
alter table sales
    drop constraint sales_products_product_id_fk;

-- Удалите первичный ключ из products
alter table products 
    drop constraint products_pk,

-- Добавьте новое поле id для суррогантного ключа в products
    add column id serial not null,

-- Сделайте данное поле первичным ключом
    add constraint products_pk primary key (id),

-- Добавьте дату начала действия записи в products
    add column valid_from timestamptz,

-- Добавьте дату окончания действия записи в products
    add column valid_to timestamptz;

-- Добавьте новый внешний ключ sales_products_id_fk в sales
alter table sales
    add constraint sales_products_id_fk 
        foreign key (product_id) references products (id);

-- Двигайтесь дальше! Ваш код: i6mYZs5YcY
