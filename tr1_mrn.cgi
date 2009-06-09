#!/usr/bin/env perl

use strict;
use warnings;

use CGI;
use GEC;

my $DSN = 'DBI:mysql:database=gec';
my $USER = 'ccr';

my @FIELDS_OF_INTEREST = qw(
    TYPE
    PRINCIPAL_RESULT_INTERPRETER
    PATIENT_ID_INTERNAL_ID
    PATIENT_ACCOUNT_NUMBER
    OBSERVATION_DATE/TIME
    RESULT_STATUS
    time_of_parsing
    MEDICATIONS
    ASSESSMENT_AND_PLAN
    REASON_FOR_CONSULT
    CHIEF_COMPLAINT
    Interpretation_Summary
    DISCHARGE
    PLAN
    PROCEDURE_PERFORMED
    INDICATION_FOR_PROCEDURE
    FINDINGS
    CONCLUSIONS
    CONCLUSION
    IMPRESSION
    IMPRESSIONS
    FINAL_IMPRESSION
    CLINICAL_INDICATION
);

my $GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);
my $q = CGI->new();

my $patient_id = $q->param('i');

my $type = $q->param('t');
my $pri = $q->param('p');

die "you need to provide a patient id\n" unless $patient_id;

# Get all the records for this patient.
my $record_ids = $GEC->record_ids_for_name('PATIENT_ID_INTERNAL_ID', $patient_id);

print <<"EOF";
Content-Type: text/html; charset=UTF-8

<HTML><HEAD><TITLE>Dictation Summaries</TITLE>
<STYLE type=\"text/css\">
  body {font-family: arial;}
  blockquote {font-size: .8em; color: #103090;}
  p {color: black;}
</STYLE></HEAD>
<BODY>
<TABLE><TR><TD>
EOF


# For every record related to this patient, for any duplicated
# PLACER_ORDER_NUMBER only show the one with the time_of_parsing
# that is most recent in time.
my $order_numbers = [];
foreach my $id (@$record_ids) {
    my $order_number = $GEC->value_for_record_id($id, 'PLACER_ORDER_NUMBER');
    next if _already_processed($order_number, $order_numbers);
    my $record = $GEC->unique_record(
        key_name       => 'PLACER_ORDER_NUMBER',
        key_value      => $order_number,
        uniquing_field => 'time_of_parsing'
    );
    next unless $record->{TYPE} eq $type;
    next unless $record->{PRINCIPAL_RESULT_INTERPRETER} eq $pri;
    print <<"EOF";
<BLOCKQUOTE>
<font color="salmon">
<a href="http://portal.saintpatrick.org/cgi-bin/getter.pl?id=$id" target="_child">$id</a>
</font>
<br>
EOF
    display_some_data($record);
    print "</BLOCKQUOTE>\n";
}

print "</TD></TR></TABLE></BODY></HTML>";

# end of main

# Determine if we've already processed this order number
# and already found the most recent record.
sub _already_processed {
    my $order_number = shift;
    my $order_numbers = shift;
    if (grep {"$_" eq "$order_number"} @$order_numbers) {
        return 1;
    } else {
        push @$order_numbers, $order_number;
        return 0;
    }
}


sub display_some_data {
    my $record = shift;

    my @interesting_fields = @FIELDS_OF_INTEREST;

    # Generate a list of all fields that look anything
    # like an allergy.
    my @fields = keys(%$record);
    my @allergy_fields = grep /ALLERG/, @fields;
    push @interesting_fields, @allergy_fields;

    foreach my $field (@interesting_fields) {
        if ($record->{$field}) {
            print "<BR><B>";
            my $fld = $field;
            $fld
                =~ s/(
                .*ALLERG.*|
                CLINICAL_INDICATION|
                FINDINGS|
                IMPRESSION|
                IMPRESSIONS|
                FINAL_IMPRESSION|
                PROCEDURE_PERFORMED|
                CHIEF_COMPLAINT|
                MEDICATIONS|
                CONCLUSION|
                PLAN|
                ASSESSMENT_AND_PLAN|
                DISCHARGE)
                /<P>$1/x;
            print "$fld</B>\t$$record{$field}\n";
        }
    }
}
