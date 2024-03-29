{%- macro pkey_and_fkey() -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- if full_refresh_mode == True -%}
            ALTER TABLE drop_shipping_dw.dim_customer ADD CONSTRAINT unique_cust_id_pk PRIMARY KEY (unique_cust_id);
          --alter table drop_shipping_dw.fact add constraint unique_cust_id_fk foreign key (unique_cust_id) references drop_shipping_dw.dim_customer;
            ALTER TABLE drop_shipping_dw.dim_products ADD CONSTRAINT unique_prod_id_pk PRIMARY KEY (unique_prod_id);
          --alter table drop_shipping_dw.fact add constraint unique_prod_id_fk foreign key (unique_prod_id) references drop_shipping_dw.dim_products;   
            ALTER TABLE drop_shipping_dw.dim_seller ADD CONSTRAINT unique_seller_id_pk PRIMARY KEY (unique_seller_id);
          --alter table drop_shipping_dw.fact add constraint unique_seller_id_fk foreign key (unique_seller_id) references drop_shipping_dw.dim_seller;   
            ALTER TABLE drop_shipping_dw.dim_vendor ADD CONSTRAINT unique_vendor_id_pk PRIMARY KEY (unique_vendor_id);
          --alter table drop_shipping_dw.fact add constraint unique_vendor_id_fk foreign key (unique_vendor_id) references drop_shipping_dw.dim_vendor;  
            ALTER TABLE drop_shipping_dw.fact ADD PRIMARY KEY (unique_order_id);
            ALTER TABLE drop_shipping_dw.fact DROP COLUMN order_id;
    {%- endif -%}
{%- endmacro -%}
