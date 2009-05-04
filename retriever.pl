#!/usr/bin/env perl

use strict;
use warnings;

our $DSN = 'DBI:mysql:database=gec';
our $USER = 'cdent';
our $GEC;

use GEC;
$GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);

use YAML;


# when we put, we are putting a hash with an id: the id is of the record
# in the values table the first column is the id
# the second the uuid of the key in keys table
# the third the value

# get the keyid for a field name
my $ordering_provider = $GEC->keyid_for_name('PRINCIPAL_RESULT_INTERPRETER');
print "$ordering_provider\n";

# get all the record_ids for a field name where the value of that fields
# is the one given
my $record_ids = $GEC->record_ids_for_name('PRINCIPAL_RESULT_INTERPRETER', 'SANMA');
print join("\n", @$record_ids);
print "\n";

# get all the record ids for TYPE of MCTH
# then unique all the fields from all the records
$record_ids = $GEC->record_ids_for_name('TYPE', 'MCTH');
my %fields;
foreach my $id (@$record_ids) {
    my $result = $GEC->get($id);
    map {$fields{$_}++} keys(%$result);
}
print join("\n", sort keys(%fields));
print "\n";

# for a given patient id, get all the types of records
# they have
$record_ids = $GEC->record_ids_for_name('PATIENT_ID_INTERNAL_ID', 'M000428925');
%fields = ();
foreach my $id (@$record_ids) {
    my $type = $GEC->value_for_record_id($id, 'TYPE');
    $fields{$type}++;
}
print join("\n", sort keys(%fields));
print "\n";

# for a given patient id show all the info for all that
# patient's records.
$record_ids = $GEC->record_ids_for_name('PATIENT_ID_INTERNAL_ID', 'M000428925');
display_some_records($record_ids);

# get some reports known to be duplicates and gaze upon them
$record_ids = $GEC->record_ids_for_name('FILLER_ORDER_NUMBER', 'DI20080612-0320');
display_some_records($record_ids);

# work out some way to get the most recent version of a set of orders
# WE assume that FILLER_ORDER_NUMBER is unique to a record, so multiples of 
# it mean duplicates. Then we can use the time_of_parsing to get the latest
# one, because WE also assume that reports come down the pipe in something
# akin to chronological order.
my $record = $GEC->unique_record(
    key_name       => 'FILLER_ORDER_NUMBER',
    key_value      => 'DI20080612-0320',
    uniquing_field => 'time_of_parsing'
);
display_some_data($record);

sub display_some_records {
    my $record_ids = shift;
    my @records = ();
    foreach my $id (@$record_ids) {
        my $data = $GEC->get($id);
        $data->{id} = $id;
        push(@records, $data);
    }
    foreach my $record (sort {$a->{TYPE} cmp $b->{TYPE}} @records) {
        print "$$record{id}", '#' x 24, "\n";
        # you can list any fields you want here
        # or do
        display_some_data($record);
    }
}

sub display_some_data {
    my $record = shift;
    # foreach my $field (sort keys(%$record)) {
    foreach my $field (qw(PATIENT_ID_INTERNAL_ID TYPE PLACER_ORDER_NUMBER FILLER_ORDER_NUMBER PATIENT_ACCOUNT_NUMBER PRINCIPAL_RESULT_INTERPRETER OBSERVATION_DATE/TIME OBSERVATION_END_DATE/TIME PROCEDURE_PERFORMED RESULT_STATUS time_of_message time_of_parsing)) {
        if ($record->{$field}) {
            print "$field\t$$record{$field}\n";
        }
    }
}


