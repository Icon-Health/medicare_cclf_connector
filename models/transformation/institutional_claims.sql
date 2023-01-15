with detail_line as(
    select
        {{ cast_string_or_varchar('h.cur_clm_uniq_id') }} as claim_id
        , cast(d.clm_line_num as integer) as claim_line_number
        , 'institutional' as claim_type
        , {{ cast_string_or_varchar('h.bene_mbi_id') }} as patient_id
        , {{ cast_string_or_varchar('NULL') }} as member_id
        , {{ try_to_cast_date('h.clm_from_dt', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('h.clm_thru_dt', 'YYYY-MM-DD') }} as claim_end_date
        , cast(NULL as date) as claim_line_start_date
        , cast(NULL as date) as claim_line_end_date
        , cast(NULL as date) as admission_date
        , cast(NULL as date) as discharge_date
        , {{ cast_string_or_varchar('h.clm_admsn_src_cd') }} as admit_source_code
        , {{ cast_string_or_varchar('h.clm_admsn_type_cd') }} as admit_type_code
        , {{ cast_string_or_varchar('h.bene_ptnt_stus_cd') }} as discharge_disposition_code
        , {{ cast_string_or_varchar('NULL') }} as place_of_service_code
        , {{ cast_string_or_varchar('h.clm_bill_fac_type_cd') }}
            || {{ cast_string_or_varchar('h.clm_bill_clsfctn_cd') }}
            || {{ cast_string_or_varchar('h.clm_bill_freq_cd') }}
        as bill_type_code
        , {{ cast_string_or_varchar('h.dgns_drg_cd') }} as ms_drg_code
        , {{ cast_string_or_varchar('d.clm_line_prod_rev_ctr_cd') }} as revenue_center_code
        , cast(d.clm_line_srvc_unit_qty as numeric) as service_unit_quantity
        , {{ cast_string_or_varchar('d.clm_line_hcpcs_cd') }} as hcpcs_code
        , {{ cast_string_or_varchar('d.hcpcs_1_mdfr_cd') }} as hcpcs_modifier_1
        , {{ cast_string_or_varchar('d.hcpcs_2_mdfr_cd') }} as hcpcs_modifier_2
        , {{ cast_string_or_varchar('d.hcpcs_3_mdfr_cd') }} as hcpcs_modifier_3
        , {{ cast_string_or_varchar('d.hcpcs_4_mdfr_cd') }} as hcpcs_modifier_4
        , {{ cast_string_or_varchar('d.hcpcs_5_mdfr_cd') }} as hcpcs_modifier_5
        , {{ cast_string_or_varchar('h.atndg_prvdr_npi_num') }} as rendering_npi
        , {{ cast_string_or_varchar('NULL') }} as billing_npi
        , {{ cast_string_or_varchar('h.fac_prvdr_npi_num') }} as facility_npi
        , cast(NULL as date) as paid_date
        , {{ cast_numeric('d.CLM_LINE_CVRD_PD_AMT') }} as paid_amount
        , {{ cast_numeric('NULL') }} as allowed_amount
        --, {{ cast_numeric('h.clm_mdcr_instnl_tot_chrg_amt') }} as charge_amount
        , {{ cast_numeric('NULL') }} as charge_amount
        , {{ cast_string_or_varchar('dx.dgns_prcdr_icd_ind') }} as diagnosis_code_type
        , {{ cast_string_or_varchar('h.PRNCPL_DGNS_CD') }} as diagnosis_code_1
        , {{ cast_string_or_varchar('dx.diagnosis_code_2') }} as diagnosis_code_2
        , {{ cast_string_or_varchar('dx.diagnosis_code_3') }} as diagnosis_code_3
        , {{ cast_string_or_varchar('dx.diagnosis_code_4') }} as diagnosis_code_4
        , {{ cast_string_or_varchar('dx.diagnosis_code_5') }} as diagnosis_code_5
        , {{ cast_string_or_varchar('dx.diagnosis_code_6') }} as diagnosis_code_6
        , {{ cast_string_or_varchar('dx.diagnosis_code_7') }} as diagnosis_code_7
        , {{ cast_string_or_varchar('dx.diagnosis_code_8') }} as diagnosis_code_8
        , {{ cast_string_or_varchar('dx.diagnosis_code_9') }} as diagnosis_code_9
        , {{ cast_string_or_varchar('dx.diagnosis_code_10') }} as diagnosis_code_10
        , {{ cast_string_or_varchar('dx.diagnosis_code_11') }} as diagnosis_code_11
        , {{ cast_string_or_varchar('dx.diagnosis_code_12') }} as diagnosis_code_12
        , {{ cast_string_or_varchar('dx.diagnosis_code_13') }} as diagnosis_code_13
        , {{ cast_string_or_varchar('dx.diagnosis_code_14') }} as diagnosis_code_14
        , {{ cast_string_or_varchar('dx.diagnosis_code_15') }} as diagnosis_code_15
        , {{ cast_string_or_varchar('dx.diagnosis_code_16') }} as diagnosis_code_16
        , {{ cast_string_or_varchar('dx.diagnosis_code_17') }} as diagnosis_code_17
        , {{ cast_string_or_varchar('dx.diagnosis_code_18') }} as diagnosis_code_18
        , {{ cast_string_or_varchar('dx.diagnosis_code_19') }} as diagnosis_code_19
        , {{ cast_string_or_varchar('dx.diagnosis_code_20') }} as diagnosis_code_20
        , {{ cast_string_or_varchar('dx.diagnosis_code_21') }} as diagnosis_code_21
        , {{ cast_string_or_varchar('dx.diagnosis_code_22') }} as diagnosis_code_22
        , {{ cast_string_or_varchar('dx.diagnosis_code_23') }} as diagnosis_code_23
        , {{ cast_string_or_varchar('dx.diagnosis_code_24') }} as diagnosis_code_24
        , {{ cast_string_or_varchar('dx.diagnosis_code_25') }} as diagnosis_code_25
        , {{ cast_string_or_varchar('dx.diagnosis_poa_1') }} as diagnosis_poa_1
        , {{ cast_string_or_varchar('dx.diagnosis_poa_2') }} as diagnosis_poa_2
        , {{ cast_string_or_varchar('dx.diagnosis_poa_3') }} as diagnosis_poa_3
        , {{ cast_string_or_varchar('dx.diagnosis_poa_4') }} as diagnosis_poa_4
        , {{ cast_string_or_varchar('dx.diagnosis_poa_5') }} as diagnosis_poa_5
        , {{ cast_string_or_varchar('dx.diagnosis_poa_6') }} as diagnosis_poa_6
        , {{ cast_string_or_varchar('dx.diagnosis_poa_7') }} as diagnosis_poa_7
        , {{ cast_string_or_varchar('dx.diagnosis_poa_8') }} as diagnosis_poa_8
        , {{ cast_string_or_varchar('dx.diagnosis_poa_9') }} as diagnosis_poa_9
        , {{ cast_string_or_varchar('dx.diagnosis_poa_10') }} as diagnosis_poa_10
        , {{ cast_string_or_varchar('dx.diagnosis_poa_11') }} as diagnosis_poa_11
        , {{ cast_string_or_varchar('dx.diagnosis_poa_12') }} as diagnosis_poa_12
        , {{ cast_string_or_varchar('dx.diagnosis_poa_13') }} as diagnosis_poa_13
        , {{ cast_string_or_varchar('dx.diagnosis_poa_14') }} as diagnosis_poa_14
        , {{ cast_string_or_varchar('dx.diagnosis_poa_15') }} as diagnosis_poa_15
        , {{ cast_string_or_varchar('dx.diagnosis_poa_16') }} as diagnosis_poa_16
        , {{ cast_string_or_varchar('dx.diagnosis_poa_17') }} as diagnosis_poa_17
        , {{ cast_string_or_varchar('dx.diagnosis_poa_18') }} as diagnosis_poa_18
        , {{ cast_string_or_varchar('dx.diagnosis_poa_19') }} as diagnosis_poa_19
        , {{ cast_string_or_varchar('dx.diagnosis_poa_20') }} as diagnosis_poa_20
        , {{ cast_string_or_varchar('dx.diagnosis_poa_21') }} as diagnosis_poa_21
        , {{ cast_string_or_varchar('dx.diagnosis_poa_22') }} as diagnosis_poa_22
        , {{ cast_string_or_varchar('dx.diagnosis_poa_23') }} as diagnosis_poa_23
        , {{ cast_string_or_varchar('dx.diagnosis_poa_24') }} as diagnosis_poa_24
        , {{ cast_string_or_varchar('dx.diagnosis_poa_25') }} as diagnosis_poa_25
        , {{ cast_string_or_varchar('px.dgns_prcdr_icd_ind') }} as procedure_code_type
        , {{ cast_string_or_varchar('px.procedure_code_1') }} as procedure_code_1
        , {{ cast_string_or_varchar('px.procedure_code_2') }} as procedure_code_2
        , {{ cast_string_or_varchar('px.procedure_code_3') }} as procedure_code_3
        , {{ cast_string_or_varchar('px.procedure_code_4') }} as procedure_code_4
        , {{ cast_string_or_varchar('px.procedure_code_5') }} as procedure_code_5
        , {{ cast_string_or_varchar('px.procedure_code_6') }} as procedure_code_6
        , {{ cast_string_or_varchar('px.procedure_code_7') }} as procedure_code_7
        , {{ cast_string_or_varchar('px.procedure_code_8') }} as procedure_code_8
        , {{ cast_string_or_varchar('px.procedure_code_9') }} as procedure_code_9
        , {{ cast_string_or_varchar('px.procedure_code_10') }} as procedure_code_10
        , {{ cast_string_or_varchar('px.procedure_code_11') }} as procedure_code_11
        , {{ cast_string_or_varchar('px.procedure_code_12') }} as procedure_code_12
        , {{ cast_string_or_varchar('px.procedure_code_13') }} as procedure_code_13
        , {{ cast_string_or_varchar('px.procedure_code_14') }} as procedure_code_14
        , {{ cast_string_or_varchar('px.procedure_code_15') }} as procedure_code_15
        , {{ cast_string_or_varchar('px.procedure_code_16') }} as procedure_code_16
        , {{ cast_string_or_varchar('px.procedure_code_17') }} as procedure_code_17
        , {{ cast_string_or_varchar('px.procedure_code_18') }} as procedure_code_18
        , {{ cast_string_or_varchar('px.procedure_code_19') }} as procedure_code_19
        , {{ cast_string_or_varchar('px.procedure_code_20') }} as procedure_code_20
        , {{ cast_string_or_varchar('px.procedure_code_21') }} as procedure_code_21
        , {{ cast_string_or_varchar('px.procedure_code_22') }} as procedure_code_22
        , {{ cast_string_or_varchar('px.procedure_code_23') }} as procedure_code_23
        , {{ cast_string_or_varchar('px.procedure_code_24') }} as procedure_code_24
        , {{ cast_string_or_varchar('px.procedure_code_25') }} as procedure_code_25
        , {{ try_to_cast_date('px.procedure_date_1', 'YYYY-MM-DD') }} as procedure_date_1
        , {{ try_to_cast_date('px.procedure_date_2', 'YYYY-MM-DD') }} as procedure_date_2
        , {{ try_to_cast_date('px.procedure_date_3', 'YYYY-MM-DD') }} as procedure_date_3
        , {{ try_to_cast_date('px.procedure_date_4', 'YYYY-MM-DD') }} as procedure_date_4
        , {{ try_to_cast_date('px.procedure_date_5', 'YYYY-MM-DD') }} as procedure_date_5
        , {{ try_to_cast_date('px.procedure_date_6', 'YYYY-MM-DD') }} as procedure_date_6
        , {{ try_to_cast_date('px.procedure_date_7', 'YYYY-MM-DD') }} as procedure_date_7
        , {{ try_to_cast_date('px.procedure_date_8', 'YYYY-MM-DD') }} as procedure_date_8
        , {{ try_to_cast_date('px.procedure_date_9', 'YYYY-MM-DD') }} as procedure_date_9
        , {{ try_to_cast_date('px.procedure_date_10', 'YYYY-MM-DD') }} as procedure_date_10
        , {{ try_to_cast_date('px.procedure_date_11', 'YYYY-MM-DD') }} as procedure_date_11
        , {{ try_to_cast_date('px.procedure_date_12', 'YYYY-MM-DD') }} as procedure_date_12
        , {{ try_to_cast_date('px.procedure_date_13', 'YYYY-MM-DD') }} as procedure_date_13
        , {{ try_to_cast_date('px.procedure_date_14', 'YYYY-MM-DD') }} as procedure_date_14
        , {{ try_to_cast_date('px.procedure_date_15', 'YYYY-MM-DD') }} as procedure_date_15
        , {{ try_to_cast_date('px.procedure_date_16', 'YYYY-MM-DD') }} as procedure_date_16
        , {{ try_to_cast_date('px.procedure_date_17', 'YYYY-MM-DD') }} as procedure_date_17
        , {{ try_to_cast_date('px.procedure_date_18', 'YYYY-MM-DD') }} as procedure_date_18
        , {{ try_to_cast_date('px.procedure_date_19', 'YYYY-MM-DD') }} as procedure_date_19
        , {{ try_to_cast_date('px.procedure_date_20', 'YYYY-MM-DD') }} as procedure_date_20
        , {{ try_to_cast_date('px.procedure_date_21', 'YYYY-MM-DD') }} as procedure_date_21
        , {{ try_to_cast_date('px.procedure_date_22', 'YYYY-MM-DD') }} as procedure_date_22
        , {{ try_to_cast_date('px.procedure_date_23', 'YYYY-MM-DD') }} as procedure_date_23
        , {{ try_to_cast_date('px.procedure_date_24', 'YYYY-MM-DD') }} as procedure_date_24
        , {{ try_to_cast_date('px.procedure_date_25', 'YYYY-MM-DD') }} as procedure_date_25
        , 'cclf' as data_source
    from {{ var('parta_claims_header')}} h
    inner join {{ var('parta_claims_revenue_center_detail')}} d
        on h.cur_clm_uniq_id = d.cur_clm_uniq_id
    left join {{ ref('procedure_pivot')}} px
        on h.cur_clm_uniq_id = px.cur_clm_uniq_id
    left join {{ ref('diagnosis_pivot')}} dx
        on h.cur_clm_uniq_id = dx.cur_clm_uniq_id
),
header_only as (
    select       
        {{ cast_string_or_varchar('cur_clm_uniq_id') }} as claim_id
        , cast (0 as integer) as claim_line_number
        , 'institutional' as claim_type
        , {{ cast_string_or_varchar('bene_mbi_id') }} as patient_id
        , {{ cast_string_or_varchar('NULL') }} as member_id
        , {{ try_to_cast_date('clm_from_dt', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('clm_thru_dt', 'YYYY-MM-DD') }} as claim_end_date
        , cast(NULL as date) as claim_line_start_date
        , cast(NULL as date) as claim_line_end_date
        , cast(NULL as date) as admission_date
        , cast(NULL as date) as discharge_date
        , {{ cast_string_or_varchar('clm_admsn_src_cd') }} as admit_source_code
        , {{ cast_string_or_varchar('clm_admsn_type_cd') }} as admit_type_code
        , {{ cast_string_or_varchar('bene_ptnt_stus_cd') }} as discharge_disposition_code
        , {{ cast_string_or_varchar('NULL') }} as place_of_service_code
        , cast (bill_type_code as string) as bill_type_code
        , {{ cast_string_or_varchar('dgns_drg_cd') }} as ms_drg_code
        , cast (NULL as string) as revenue_center_code
        , cast (NULL as numeric) as service_unit_quantity
        , cast (NULL as string) as hcpcs_code
        , cast (NULL as string) as hcpcs_modifier_1
        , cast (NULL as string) as hcpcs_modifier_2
        , cast (NULL as string) as hcpcs_modifier_3
        , cast (NULL as string) as hcpcs_modifier_4
        , cast (NULL as string) as hcpcs_modifier_5
        , cast (NULL as string) as rendering_npi
        , cast (NULL as string) as billing_npi
        , cast (NULL as string) as facility_npi
        , cast(NULL as date) as paid_date
        , {{ cast_numeric('CLM_PMT_AMT') }} as paid_amount
        , {{ cast_numeric('NULL') }} as allowed_amount
        , {{ cast_numeric('NULL') }} as charge_amount
        , cast (NULL as string) as diagnosis_code_type     
        , cast (PRNCPL_DGNS_CD as string) as diagnosis_code_1        
        , cast (NULL as string) as diagnosis_code_2        
        , cast (NULL as string) as diagnosis_code_3        
        , cast (NULL as string) as diagnosis_code_4        
        , cast (NULL as string) as diagnosis_code_5        
        , cast (NULL as string) as diagnosis_code_6        
        , cast (NULL as string) as diagnosis_code_7        
        , cast (NULL as string) as diagnosis_code_8        
        , cast (NULL as string) as diagnosis_code_9        
        , cast (NULL as string) as diagnosis_code_10       
        , cast (NULL as string) as diagnosis_code_11       
        , cast (NULL as string) as diagnosis_code_12       
        , cast (NULL as string) as diagnosis_code_13       
        , cast (NULL as string) as diagnosis_code_14       
        , cast (NULL as string) as diagnosis_code_15       
        , cast (NULL as string) as diagnosis_code_16       
        , cast (NULL as string) as diagnosis_code_17       
        , cast (NULL as string) as diagnosis_code_18       
        , cast (NULL as string) as diagnosis_code_19       
        , cast (NULL as string) as diagnosis_code_20       
        , cast (NULL as string) as diagnosis_code_21       
        , cast (NULL as string) as diagnosis_code_22       
        , cast (NULL as string) as diagnosis_code_23       
        , cast (NULL as string) as diagnosis_code_24       
        , cast (NULL as string) as diagnosis_code_25       
        , cast (NULL as string) as diagnosis_poa_1         
        , cast (NULL as string) as diagnosis_poa_2         
        , cast (NULL as string) as diagnosis_poa_3         
        , cast (NULL as string) as diagnosis_poa_4         
        , cast (NULL as string) as diagnosis_poa_5         
        , cast (NULL as string) as diagnosis_poa_6         
        , cast (NULL as string) as diagnosis_poa_7         
        , cast (NULL as string) as diagnosis_poa_8         
        , cast (NULL as string) as diagnosis_poa_9         
        , cast (NULL as string) as diagnosis_poa_10        
        , cast (NULL as string) as diagnosis_poa_11        
        , cast (NULL as string) as diagnosis_poa_12        
        , cast (NULL as string) as diagnosis_poa_13        
        , cast (NULL as string) as diagnosis_poa_14        
        , cast (NULL as string) as diagnosis_poa_15        
        , cast (NULL as string) as diagnosis_poa_16        
        , cast (NULL as string) as diagnosis_poa_17        
        , cast (NULL as string) as diagnosis_poa_18        
        , cast (NULL as string) as diagnosis_poa_19        
        , cast (NULL as string) as diagnosis_poa_20        
        , cast (NULL as string) as diagnosis_poa_21        
        , cast (NULL as string) as diagnosis_poa_22        
        , cast (NULL as string) as diagnosis_poa_23        
        , cast (NULL as string) as diagnosis_poa_24        
        , cast (NULL as string) as diagnosis_poa_25        
        , cast (NULL as string) as procedure_code_type     
        , cast (NULL as string) as procedure_code_1        
        , cast (NULL as string) as procedure_code_2        
        , cast (NULL as string) as procedure_code_3        
        , cast (NULL as string) as procedure_code_4        
        , cast (NULL as string) as procedure_code_5        
        , cast (NULL as string) as procedure_code_6        
        , cast (NULL as string) as procedure_code_7        
        , cast (NULL as string) as procedure_code_8        
        , cast (NULL as string) as procedure_code_9        
        , cast (NULL as string) as procedure_code_10       
        , cast (NULL as string) as procedure_code_11       
        , cast (NULL as string) as procedure_code_12       
        , cast (NULL as string) as procedure_code_13       
        , cast (NULL as string) as procedure_code_14       
        , cast (NULL as string) as procedure_code_15       
        , cast (NULL as string) as procedure_code_16       
        , cast (NULL as string) as procedure_code_17       
        , cast (NULL as string) as procedure_code_18       
        , cast (NULL as string) as procedure_code_19       
        , cast (NULL as string) as procedure_code_20       
        , cast (NULL as string) as procedure_code_21       
        , cast (NULL as string) as procedure_code_22       
        , cast (NULL as string) as procedure_code_23       
        , cast (NULL as string) as procedure_code_24       
        , cast (NULL as string) as procedure_code_25       
        , cast (NULL as date) as procedure_date_1          
        , cast (NULL as date) as procedure_date_2          
        , cast (NULL as date) as procedure_date_3          
        , cast (NULL as date) as procedure_date_4          
        , cast (NULL as date) as procedure_date_5          
        , cast (NULL as date) as procedure_date_6          
        , cast (NULL as date) as procedure_date_7          
        , cast (NULL as date) as procedure_date_8          
        , cast (NULL as date) as procedure_date_9          
        , cast (NULL as date) as procedure_date_10         
        , cast (NULL as date) as procedure_date_11         
        , cast (NULL as date) as procedure_date_12         
        , cast (NULL as date) as procedure_date_13         
        , cast (NULL as date) as procedure_date_14         
        , cast (NULL as date) as procedure_date_15         
        , cast (NULL as date) as procedure_date_16         
        , cast (NULL as date) as procedure_date_17         
        , cast (NULL as date) as procedure_date_18         
        , cast (NULL as date) as procedure_date_19         
        , cast (NULL as date) as procedure_date_20         
        , cast (NULL as date) as procedure_date_21         
        , cast (NULL as date) as procedure_date_22         
        , cast (NULL as date) as procedure_date_23         
        , cast (NULL as date) as procedure_date_24         
        , cast (NULL as date) as procedure_date_25         
        , 'cclf' as data_source
    from (
        select  h.CUR_CLM_UNIQ_ID, h.CLM_PMT_AMT, h.bene_mbi_id, h.clm_from_dt, h.clm_thru_dt, h.clm_admsn_src_cd,h.clm_admsn_type_cd, h.bene_ptnt_stus_cd, h.dgns_drg_cd
        ,cast( clm_bill_fac_type_cd as string ) || cast( clm_bill_clsfctn_cd as string )|| cast( clm_bill_clsfctn_cd as string ) as bill_type_code
        ,sum(d.CLM_LINE_CVRD_PD_AMT) as sumline, h.PRNCPL_DGNS_CD
        from `fiery-pipe-330412.auxilium.parta_claims_header`  h
        inner join `fiery-pipe-330412.auxilium.parta_claims_revenue_center_detail`  d
        on h.cur_clm_uniq_id = d.cur_clm_uniq_id
        group by h.CUR_CLM_UNIQ_ID, h.CLM_PMT_AMT, h.bene_mbi_id, h.clm_from_dt, h.clm_thru_dt, h.clm_admsn_src_cd, h.clm_admsn_type_cd, h.bene_ptnt_stus_cd, clm_bill_fac_type_cd, clm_bill_clsfctn_cd, clm_bill_clsfctn_cd, h.dgns_drg_cd, h.PRNCPL_DGNS_CD
        having sumline = 0
    )
), 

final as (
    select 
        claim_id	
        ,claim_line_number	
        ,claim_type	
        ,patient_id	
        ,member_id	
        ,claim_start_date	
        ,claim_end_date	
        ,claim_line_start_date	
        ,claim_line_end_date	
        ,admission_date	
        ,discharge_date	
        ,admit_source_code	
        ,admit_type_code	
        ,discharge_disposition_code	
        ,place_of_service_code
        ,bill_type_code	
        ,ms_drg_code	
        ,revenue_center_code	
        ,service_unit_quantity	
        ,hcpcs_code	
        ,hcpcs_modifier_1	
        ,hcpcs_modifier_2	
        ,hcpcs_modifier_3	
        ,hcpcs_modifier_4	
        ,hcpcs_modifier_5	
        ,rendering_npi	
        ,billing_npi	
        ,facility_npi	
        ,paid_date	
        ,paid_amount	
        ,allowed_amount	
        ,charge_amount	
        ,diagnosis_code_type	
        ,diagnosis_code_1	
        ,diagnosis_code_2	
        ,diagnosis_code_3	
        ,diagnosis_code_4	
        ,diagnosis_code_5	
        ,diagnosis_code_6	
        ,diagnosis_code_7	
        ,diagnosis_code_8	
        ,diagnosis_code_9	
        ,diagnosis_code_10	
        ,diagnosis_code_11	
        ,diagnosis_code_12	
        ,diagnosis_code_13	
        ,diagnosis_code_14	
        ,diagnosis_code_15	
        ,diagnosis_code_16	
        ,diagnosis_code_17	
        ,diagnosis_code_18	
        ,diagnosis_code_19	
        ,diagnosis_code_20	
        ,diagnosis_code_21	
        ,diagnosis_code_22	
        ,diagnosis_code_23	
        ,diagnosis_code_24	
        ,diagnosis_code_25	
        ,diagnosis_poa_1	
        ,diagnosis_poa_2	
        ,diagnosis_poa_3	
        ,diagnosis_poa_4	
        ,diagnosis_poa_5	
        ,diagnosis_poa_6	
        ,diagnosis_poa_7	
        ,diagnosis_poa_8	
        ,diagnosis_poa_9	
        ,diagnosis_poa_10	
        ,diagnosis_poa_11	
        ,diagnosis_poa_12	
        ,diagnosis_poa_13	
        ,diagnosis_poa_14	
        ,diagnosis_poa_15	
        ,diagnosis_poa_16	
        ,diagnosis_poa_17	
        ,diagnosis_poa_18	
        ,diagnosis_poa_19	
        ,diagnosis_poa_20	
        ,diagnosis_poa_21	
        ,diagnosis_poa_22	
        ,diagnosis_poa_23	
        ,diagnosis_poa_24	
        ,diagnosis_poa_25	
        ,procedure_code_type	
        ,procedure_code_1	
        ,procedure_code_2	
        ,procedure_code_3	
        ,procedure_code_4	
        ,procedure_code_5	
        ,procedure_code_6	
        ,procedure_code_7	
        ,procedure_code_8	
        ,procedure_code_9	
        ,procedure_code_10	
        ,procedure_code_11	
        ,procedure_code_12	
        ,procedure_code_13	
        ,procedure_code_14	
        ,procedure_code_15	
        ,procedure_code_16	
        ,procedure_code_17	
        ,procedure_code_18	
        ,procedure_code_19	
        ,procedure_code_20	
        ,procedure_code_21	
        ,procedure_code_22	
        ,procedure_code_23	
        ,procedure_code_24	
        ,procedure_code_25	
        ,procedure_date_1	
        ,procedure_date_2	
        ,procedure_date_3	
        ,procedure_date_4	
        ,procedure_date_5	
        ,procedure_date_6	
        ,procedure_date_7	
        ,procedure_date_8	
        ,procedure_date_9	
        ,procedure_date_10	
        ,procedure_date_11	
        ,procedure_date_12	
        ,procedure_date_13	
        ,procedure_date_14	
        ,procedure_date_15	
        ,procedure_date_16	
        ,procedure_date_17	
        ,procedure_date_18	
        ,procedure_date_19	
        ,procedure_date_20	
        ,procedure_date_21	
        ,procedure_date_22	
        ,procedure_date_23	
        ,procedure_date_24	
        ,procedure_date_25	
        ,data_source
    from header_only
    union all
    select 
        claim_id	
        ,claim_line_number	
        ,claim_type	
        ,patient_id	
        ,member_id	
        ,claim_start_date	
        ,claim_end_date	
        ,claim_line_start_date	
        ,claim_line_end_date	
        ,admission_date	
        ,discharge_date	
        ,admit_source_code	
        ,admit_type_code	
        ,discharge_disposition_code	
        ,place_of_service_code
        ,bill_type_code	
        ,ms_drg_code	
        ,revenue_center_code	
        ,service_unit_quantity	
        ,hcpcs_code	
        ,hcpcs_modifier_1	
        ,hcpcs_modifier_2	
        ,hcpcs_modifier_3	
        ,hcpcs_modifier_4	
        ,hcpcs_modifier_5	
        ,rendering_npi	
        ,billing_npi	
        ,facility_npi	
        ,paid_date	
        ,paid_amount	
        ,allowed_amount	
        ,charge_amount	
        ,diagnosis_code_type	
        ,diagnosis_code_1	
        ,diagnosis_code_2	
        ,diagnosis_code_3	
        ,diagnosis_code_4	
        ,diagnosis_code_5	
        ,diagnosis_code_6	
        ,diagnosis_code_7	
        ,diagnosis_code_8	
        ,diagnosis_code_9	
        ,diagnosis_code_10	
        ,diagnosis_code_11	
        ,diagnosis_code_12	
        ,diagnosis_code_13	
        ,diagnosis_code_14	
        ,diagnosis_code_15	
        ,diagnosis_code_16	
        ,diagnosis_code_17	
        ,diagnosis_code_18	
        ,diagnosis_code_19	
        ,diagnosis_code_20	
        ,diagnosis_code_21	
        ,diagnosis_code_22	
        ,diagnosis_code_23	
        ,diagnosis_code_24	
        ,diagnosis_code_25	
        ,diagnosis_poa_1	
        ,diagnosis_poa_2	
        ,diagnosis_poa_3	
        ,diagnosis_poa_4	
        ,diagnosis_poa_5	
        ,diagnosis_poa_6	
        ,diagnosis_poa_7	
        ,diagnosis_poa_8	
        ,diagnosis_poa_9	
        ,diagnosis_poa_10	
        ,diagnosis_poa_11	
        ,diagnosis_poa_12	
        ,diagnosis_poa_13	
        ,diagnosis_poa_14	
        ,diagnosis_poa_15	
        ,diagnosis_poa_16	
        ,diagnosis_poa_17	
        ,diagnosis_poa_18	
        ,diagnosis_poa_19	
        ,diagnosis_poa_20	
        ,diagnosis_poa_21	
        ,diagnosis_poa_22	
        ,diagnosis_poa_23	
        ,diagnosis_poa_24	
        ,diagnosis_poa_25	
        ,procedure_code_type	
        ,procedure_code_1	
        ,procedure_code_2	
        ,procedure_code_3	
        ,procedure_code_4	
        ,procedure_code_5	
        ,procedure_code_6	
        ,procedure_code_7	
        ,procedure_code_8	
        ,procedure_code_9	
        ,procedure_code_10	
        ,procedure_code_11	
        ,procedure_code_12	
        ,procedure_code_13	
        ,procedure_code_14	
        ,procedure_code_15	
        ,procedure_code_16	
        ,procedure_code_17	
        ,procedure_code_18	
        ,procedure_code_19	
        ,procedure_code_20	
        ,procedure_code_21	
        ,procedure_code_22	
        ,procedure_code_23	
        ,procedure_code_24	
        ,procedure_code_25	
        ,procedure_date_1	
        ,procedure_date_2	
        ,procedure_date_3	
        ,procedure_date_4	
        ,procedure_date_5	
        ,procedure_date_6	
        ,procedure_date_7	
        ,procedure_date_8	
        ,procedure_date_9	
        ,procedure_date_10	
        ,procedure_date_11	
        ,procedure_date_12	
        ,procedure_date_13	
        ,procedure_date_14	
        ,procedure_date_15	
        ,procedure_date_16	
        ,procedure_date_17	
        ,procedure_date_18	
        ,procedure_date_19	
        ,procedure_date_20	
        ,procedure_date_21	
        ,procedure_date_22	
        ,procedure_date_23	
        ,procedure_date_24	
        ,procedure_date_25	
        ,data_source
    from detail_line
)

select * from final