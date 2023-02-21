
{{ config(order_by='(DoiTuongVaoVien_BF)', engine='MergeTree()', materialized='incremental') }}
with aaaa as(
    with DIM as (select * from {{ref('DIM_DoiTuongVaoVien')}} )
    select toString(farmHash64(DoiTuongVaoVien_BF))as MaDoiTuongVaoVien_BF,DoiTuongVaoVien_BF, DoiTuongVaoVien, MaDoiTuongVaoVien 
    from (
        select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe, 
        ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(STAG.TenDoiTuongThanhToan,'KHÁC') as DoiTuongVaoVien_BF,DIM.DoiTuongVaoVien as DoiTuongVaoVien,DIM.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_ChiDinhDichVu as STAG   
            cross join DIM)
           )
	where STT =1 
    
    union distinct

    select toString(farmHash64(DoiTuongVaoVien_BF))as MaDoiTuongVaoVien_BF,DoiTuongVaoVien_BF, DoiTuongVaoVien, MaDoiTuongVaoVien 
    from (
        select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe, 
        ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(STAG.TenDoiTuongThanhToan,'KHÁC') as DoiTuongVaoVien_BF,DIM.DoiTuongVaoVien as DoiTuongVaoVien,DIM.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_HoaDon_ChiTietHoaDon as STAG   
            cross join DIM)
           )
	where STT =1 

    union distinct

    select toString(farmHash64(DoiTuongVaoVien_BF))as MaDoiTuongVaoVien_BF,DoiTuongVaoVien_BF, DoiTuongVaoVien, MaDoiTuongVaoVien 
    from (
        select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe, 
        ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(STAG.DoiTuongBenhNhan,'KHÁC') as DoiTuongVaoVien_BF,DIM.DoiTuongVaoVien as DoiTuongVaoVien,DIM.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_ThongTinChuyenTuyen as STAG   
            cross join DIM)
           )
	where STT =1 

    union distinct

    select toString(farmHash64(DoiTuongVaoVien_BF))as MaDoiTuongVaoVien_BF,DoiTuongVaoVien_BF, DoiTuongVaoVien, MaDoiTuongVaoVien 
    from (
        select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe, 
        ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(STAG.DoiTuongVaoVien,'KHÁC') as DoiTuongVaoVien_BF,DIM.DoiTuongVaoVien as DoiTuongVaoVien,DIM.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_ThongTinDieuTri as STAG   
            cross join DIM)
           )
	where STT =1 
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where DoiTuongVaoVien_BF not in (select distinct DoiTuongVaoVien_BF from {{this}} )

{% endif %}