TRUNCATE max_pivot;

INSERT INTO max_pivot
SELECT
    concept_id
  , MAX(max_amount) FILTER (WHERE hospital_id=1) AS gross_1
  , MAX(max_amount) FILTER (WHERE hospital_id=2) AS gross_2
  , MAX(max_amount) FILTER (WHERE hospital_id=3) AS gross_3
  , MAX(max_amount) FILTER (WHERE hospital_id=4) AS gross_4
  , MAX(max_amount) FILTER (WHERE hospital_id=5) AS gross_5
  , MAX(max_amount) FILTER (WHERE hospital_id=6) AS gross_6
  , MAX(max_amount) FILTER (WHERE hospital_id=7) AS gross_7
  , MAX(max_amount) FILTER (WHERE hospital_id=8) AS gross_8
  , MAX(max_amount) FILTER (WHERE hospital_id=9) AS gross_9
  , MAX(max_amount) FILTER (WHERE hospital_id=10) AS gross_10
  , MAX(max_amount) FILTER (WHERE hospital_id=11) AS gross_11
  , MAX(max_amount) FILTER (WHERE hospital_id=12) AS gross_12
  , MAX(max_amount) FILTER (WHERE hospital_id=13) AS gross_13
  , MAX(max_amount) FILTER (WHERE hospital_id=14) AS gross_14
  , MAX(max_amount) FILTER (WHERE hospital_id=15) AS gross_15
  , MAX(max_amount) FILTER (WHERE hospital_id=16) AS gross_16
  , MAX(max_amount) FILTER (WHERE hospital_id=17) AS gross_17
  , MAX(max_amount) FILTER (WHERE hospital_id=18) AS gross_18
  , MAX(max_amount) FILTER (WHERE hospital_id=19) AS gross_19
  , MAX(max_amount) FILTER (WHERE hospital_id=20) AS gross_20
  , MAX(max_amount) FILTER (WHERE hospital_id=21) AS gross_21
  , MAX(max_amount) FILTER (WHERE hospital_id=22) AS gross_22
  , MAX(max_amount) FILTER (WHERE hospital_id=23) AS gross_23
  , MAX(max_amount) FILTER (WHERE hospital_id=24) AS gross_24
  , MAX(max_amount) FILTER (WHERE hospital_id=25) AS gross_25
  , MAX(max_amount) FILTER (WHERE hospital_id=26) AS gross_26
  , MAX(max_amount) FILTER (WHERE hospital_id=27) AS gross_27
  , MAX(max_amount) FILTER (WHERE hospital_id=28) AS gross_28
  , MAX(max_amount) FILTER (WHERE hospital_id=29) AS gross_29
  , MAX(max_amount) FILTER (WHERE hospital_id=30) AS gross_30
  , MAX(max_amount) FILTER (WHERE hospital_id=31) AS gross_31
  , MAX(max_amount) FILTER (WHERE hospital_id=32) AS gross_32
  , MAX(max_amount) FILTER (WHERE hospital_id=33) AS gross_33
  , MAX(max_amount) FILTER (WHERE hospital_id=34) AS gross_34
  , MAX(max_amount) FILTER (WHERE hospital_id=35) AS gross_35
  , MAX(max_amount) FILTER (WHERE hospital_id=36) AS gross_36
  , MAX(max_amount) FILTER (WHERE hospital_id=37) AS gross_37
  , MAX(max_amount) FILTER (WHERE hospital_id=38) AS gross_38
  , MAX(max_amount) FILTER (WHERE hospital_id=39) AS gross_39
  , MAX(max_amount) FILTER (WHERE hospital_id=40) AS gross_40
  , MAX(max_amount) FILTER (WHERE hospital_id=41) AS gross_41
  , MAX(max_amount) FILTER (WHERE hospital_id=42) AS gross_42
  , MAX(max_amount) FILTER (WHERE hospital_id=43) AS gross_43
  , MAX(max_amount) FILTER (WHERE hospital_id=44) AS gross_44
  , MAX(max_amount) FILTER (WHERE hospital_id=45) AS gross_45
  , MAX(max_amount) FILTER (WHERE hospital_id=46) AS gross_46
  , MAX(max_amount) FILTER (WHERE hospital_id=47) AS gross_47
  , MAX(max_amount) FILTER (WHERE hospital_id=48) AS gross_48
  , MAX(max_amount) FILTER (WHERE hospital_id=49) AS gross_49
  , MAX(max_amount) FILTER (WHERE hospital_id=50) AS gross_50
  , MAX(max_amount) FILTER (WHERE hospital_id=51) AS gross_51
  , MAX(max_amount) FILTER (WHERE hospital_id=52) AS gross_52
  , MAX(max_amount) FILTER (WHERE hospital_id=53) AS gross_53
  , MAX(max_amount) FILTER (WHERE hospital_id=54) AS gross_54
  , MAX(max_amount) FILTER (WHERE hospital_id=55) AS gross_55
  , MAX(max_amount) FILTER (WHERE hospital_id=56) AS gross_56
  , MAX(max_amount) FILTER (WHERE hospital_id=57) AS gross_57
  , MAX(max_amount) FILTER (WHERE hospital_id=58) AS gross_58
  , MAX(max_amount) FILTER (WHERE hospital_id=59) AS gross_59
  , MAX(max_amount) FILTER (WHERE hospital_id=60) AS gross_60
  , MAX(max_amount) FILTER (WHERE hospital_id=61) AS gross_61
  , MAX(max_amount) FILTER (WHERE hospital_id=62) AS gross_62
  , MAX(max_amount) FILTER (WHERE hospital_id=63) AS gross_63
  , MAX(max_amount) FILTER (WHERE hospital_id=64) AS gross_64
  , MAX(max_amount) FILTER (WHERE hospital_id=65) AS gross_65
  , MAX(max_amount) FILTER (WHERE hospital_id=66) AS gross_66
  , MAX(max_amount) FILTER (WHERE hospital_id=67) AS gross_67
  , MAX(max_amount) FILTER (WHERE hospital_id=68) AS gross_68
  , MAX(max_amount) FILTER (WHERE hospital_id=69) AS gross_69
  , MAX(max_amount) FILTER (WHERE hospital_id=70) AS gross_70
  , MAX(max_amount) FILTER (WHERE hospital_id=71) AS gross_71
  , MAX(max_amount) FILTER (WHERE hospital_id=72) AS gross_72
  , MAX(max_amount) FILTER (WHERE hospital_id=73) AS gross_73
  , MAX(max_amount) FILTER (WHERE hospital_id=74) AS gross_74
  , MAX(max_amount) FILTER (WHERE hospital_id=75) AS gross_75
  , MAX(max_amount) FILTER (WHERE hospital_id=76) AS gross_76
  , MAX(max_amount) FILTER (WHERE hospital_id=77) AS gross_77
  , MAX(max_amount) FILTER (WHERE hospital_id=78) AS gross_78
  , MAX(max_amount) FILTER (WHERE hospital_id=79) AS gross_79
  , MAX(max_amount) FILTER (WHERE hospital_id=80) AS gross_80
  , MAX(max_amount) FILTER (WHERE hospital_id=81) AS gross_81
  , MAX(max_amount) FILTER (WHERE hospital_id=82) AS gross_82
  , MAX(max_amount) FILTER (WHERE hospital_id=83) AS gross_83
  , MAX(max_amount) FILTER (WHERE hospital_id=84) AS gross_84
  , MAX(max_amount) FILTER (WHERE hospital_id=85) AS gross_85
  , MAX(max_amount) FILTER (WHERE hospital_id=86) AS gross_86
  , MAX(max_amount) FILTER (WHERE hospital_id=87) AS gross_87
  , MAX(max_amount) FILTER (WHERE hospital_id=88) AS gross_88
  , MAX(max_amount) FILTER (WHERE hospital_id=89) AS gross_89
  , MAX(max_amount) FILTER (WHERE hospital_id=90) AS gross_90
  , MAX(max_amount) FILTER (WHERE hospital_id=91) AS gross_91
  , MAX(max_amount) FILTER (WHERE hospital_id=92) AS gross_92
  , MAX(max_amount) FILTER (WHERE hospital_id=93) AS gross_93
  , MAX(max_amount) FILTER (WHERE hospital_id=94) AS gross_94
  , MAX(max_amount) FILTER (WHERE hospital_id=95) AS gross_95
  , MAX(max_amount) FILTER (WHERE hospital_id=96) AS gross_96
  , MAX(max_amount) FILTER (WHERE hospital_id=97) AS gross_97
  , MAX(max_amount) FILTER (WHERE hospital_id=98) AS gross_98
  , MAX(max_amount) FILTER (WHERE hospital_id=99) AS gross_99
  , MAX(max_amount) FILTER (WHERE hospital_id=100) AS gross_100
  , MAX(max_amount) FILTER (WHERE hospital_id=101) AS gross_101
  , MAX(max_amount) FILTER (WHERE hospital_id=102) AS gross_102
  , MAX(max_amount) FILTER (WHERE hospital_id=103) AS gross_103
  , MAX(max_amount) FILTER (WHERE hospital_id=104) AS gross_104
  , MAX(max_amount) FILTER (WHERE hospital_id=105) AS gross_105
  , MAX(max_amount) FILTER (WHERE hospital_id=106) AS gross_106
  , MAX(max_amount) FILTER (WHERE hospital_id=107) AS gross_107
  , MAX(max_amount) FILTER (WHERE hospital_id=108) AS gross_108
  , MAX(max_amount) FILTER (WHERE hospital_id=109) AS gross_109
  , MAX(max_amount) FILTER (WHERE hospital_id=110) AS gross_110
  , MAX(max_amount) FILTER (WHERE hospital_id=111) AS gross_111
  , MAX(max_amount) FILTER (WHERE hospital_id=112) AS gross_112
  , MAX(max_amount) FILTER (WHERE hospital_id=113) AS gross_113
  , MAX(max_amount) FILTER (WHERE hospital_id=114) AS gross_114
  , MAX(max_amount) FILTER (WHERE hospital_id=115) AS gross_115
FROM public.price_pivot
GROUP BY
    concept_id
;

COPY max_pivot TO PROGRAM 'gzip > /opt/data/extracts/max_pivot.csv.gz' WITH CSV HEADER;
