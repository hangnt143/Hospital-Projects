{{ config(order_by='(MaKhoa_BF)', engine='MergeTree()', materialized='incremental') }}
with aaaa as(
    with DIM as (select * from {{ref('DIM_Khoa')}})
    select distinct toString(farmHash64(TenKhoa_BF)) as MaKhoa_BF, TenKhoa_BF as TenKhoa_BF,MaKhoa,TenKhoa from 
    (select TenKhoa_BF,TenKhoa,MaKhoa,ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa)) as TyLe,
    ROW_NUMBER() over(partition by TenKhoa_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa))) as STT
                from ( 
                select distinct coalesce(STAG.Khoa ,'KHÁC') as TenKhoa_BF, 
                DIM.TenKhoa  as TenKhoa ,DIM.MaKhoa  as MaKhoa 
                from STAGING_ThongTinDieuTri_ThongTinDieuTriKhoa_ThongTinDieuTriPhong as STAG   
                cross join
                DIM)a) b
    where STT = 1

    union distinct

    select distinct toString(farmHash64(TenKhoa_BF)) as MaKhoa_BF, TenKhoa_BF as TenKhoa_BF,MaKhoa,TenKhoa from 
    (select TenKhoa_BF,TenKhoa,MaKhoa,ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa)) as TyLe,
    ROW_NUMBER() over(partition by TenKhoa_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa))) as STT
                from ( 
                select distinct coalesce(STAG.TenKhoaChiDinh ,'KHÁC') as TenKhoa_BF, 
                DIM.TenKhoa  as TenKhoa ,DIM.MaKhoa  as MaKhoa 
                from STAGING_HoaDon_ChiTietHoaDon as STAG   
                cross join
                DIM)a) b
    where STT = 1 
    union distinct

    select distinct toString(farmHash64(TenKhoa_BF)) as MaKhoa_BF, TenKhoa_BF as TenKhoa_BF,MaKhoa,TenKhoa from 
    (select TenKhoa_BF,TenKhoa,MaKhoa,ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa)) as TyLe,
    ROW_NUMBER() over(partition by TenKhoa_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa))) as STT
                from ( 
                select distinct coalesce(STAG.TenKhoaThuTien ,'KHÁC') as TenKhoa_BF, 
                DIM.TenKhoa  as TenKhoa ,DIM.MaKhoa  as MaKhoa 
                from STAGING_HoaDon_ChiTietHoaDon as STAG   
                cross join
                DIM)a) b
    where STT = 1 
    )

select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where MaKhoa_BF not in (select distinct MaKhoa_BF from {{this}} )

{% endif %}