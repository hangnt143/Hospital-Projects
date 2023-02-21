
{{ config(order_by='(ThongTinDieuTriPhongID)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(
select
ifNull(_id,'-1')	as ThongTinDieuTriPhongID,												
							
toString(farmHash64("PhongKham","Khoa")) as`MaPhong` ,

coalesce (PhongKham,'-1') as PhongKham,

coalesce(Phong,'-1') as Phong,

							
DieuTriKhoaID as`ThongTinDieuTriKhoaID` ,						
							
`ThongTinDieuTriID` ,							
							
`MaCSKCB` ,							
							
-- coalesce("ThoiGianTiepDon",0) as `ThoiGianTiepDon`,			
coalesce(toInt64(substring(toString(ThoiGianTiepDon),1,8)),0) as `ThoiGianTiepDon` ,				
							
-- coalesce("ThoiGianBatDau",0) as `ThoiGianBatDau`,		
coalesce(toInt64(substring(toString(ThoiGianBatDau),1,8)),0) as `ThoiGianBatDau`,					
							
`MaChanDoanRaPhong` ,							
							
-- coalesce("ThoiGianKetThuc",0) as `ThoiGianKetThuc`,

coalesce(toInt64(substring(toString(ThoiGianKetThuc),1,8)),0) as `ThoiGianKetThuc`	,						
							
coalesce("NgayDongBo",0) as `NgayDongBo`  ,	
-- NgayDongBo, 
TimeCreate,						
							
MAPPING_Khoa.MaKhoa as `MaKhoa` ,							
							
MAPPING_BenhAn.`MaLoaiBenhAn` as `MaLoaiBenhAn` ,							
							
MAPPING_DoiTuongVaoVien.`MaDoiTuongVaoVien`as `MaDoiTuongVaoVien` ,							
case							
	when s.ThoiGianTiepDon is null  then 0						
	when s.ThoiGianBatDau is null then 0	
	when s.ThoiGianTiepDon <10000000  then 0
	when s.ThoiGianBatDau <10000000 then 0
	when  DATE_DIFF('minute',toDateTimeFromInt(s.ThoiGianTiepDon), toDateTimeFromInt(s.ThoiGianBatDau)) < 0 then 0						
	else DATE_DIFF('minute',toDateTimeFromInt(s.ThoiGianTiepDon), toDateTimeFromInt(s.ThoiGianBatDau))						
	end as SoPhutChoKham,						
case							
	when s.ThoiGianBatDau is null then 0						
	when s.ThoiGianKetThuc is null then 0
	when s.ThoiGianBatDau <10000000  then 0
	when s.ThoiGianKetThuc <10000000 then 0
	when  DATE_DIFF('minute', toDateTimeFromInt(s.ThoiGianBatDau),toDateTimeFromInt(s.ThoiGianKetThuc)) < 0 then 0						
	else DATE_DIFF('minute', toDateTimeFromInt(s.ThoiGianBatDau),toDateTimeFromInt(s.ThoiGianKetThuc))					
	end as SoPhutKhamBenh							
    from STAGING_ThongTinDieuTri_ThongTinDieuTriKhoa_ThongTinDieuTriPhong as s
left join {{ref('MAPPING_DoiTuongVaoVien')}}  on MAPPING_DoiTuongVaoVien.DoiTuongVaoVien_BF = COALESCE (s.DoiTuongVaoVien,'KHÁC')
left join {{ref('MAPPING_BenhAn')}} on MAPPING_BenhAn.LoaiBenhAn_BF =  COALESCE (s.LoaiBenhAn,'KHÁC')
left join {{ref('MAPPING_Khoa')}}  on MAPPING_Khoa.TenKhoa_BF =  COALESCE (s.Khoa,'KHÁC')
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}
