Select Scen_Name As "LP/SCEN",Scen_Version As "VERSION",Time,Frequency,Listagg(Load_Sch.Elmnt,',') Within Group (Order By Load_Sch.Scen_Name)
AS DAY From
(Select A.Scen_Name,A.Scen_Version,To_Char(To_Date(A.S_Hour||':'||A.S_Minute||':'||A.S_Second,'HH24:MI:SS'),'HH:MI:SS AM') As Time,
Decode(S_Type,'H','Hourly','D','Daily','W','Weekly') As Frequency
,decode(trim( x.column_value.extract( 'e/text()' ) ),'1','Sunday','2','Monday','3','Tuesday','4','Wednesday','5','Thursday','6','Friday','7','Saturday')
As Elmnt
From Snp_Plan_Agent A
join table ( xmlsequence( xmltype('<e><e>'||replace( a.S_WEEK_DAY, ',', '</e><e>' )||'</e></e>' ).extract( 'e/e' ))) x
On ( 1=1 )
Where A.Stat_Plan='E')Load_Sch
Group By Scen_Name,Scen_Version,Time,Frequency
order by VERSION;