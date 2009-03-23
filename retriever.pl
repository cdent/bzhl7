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
print join("\n", map {$_->[0]} @$record_ids);
print "\n";

# get all the record ids for TYPE of MCTH
# then unique all the fields from all the records
$record_ids = $GEC->record_ids_for_name('TYPE', 'MCTH');
my %fields;
foreach my $row (@$record_ids) {
    my $id = $row->[0];
    my $result = $GEC->get($id);
    map {$fields{$_}++} keys(%$result);
}
print join("\n", sort keys(%fields));

print "\n";

# for a given patient id, get all the types of records
# they have
$record_ids = $GEC->record_ids_for_name('ACCOUNT_NUMBER', 'M0001176119');
%fields;
foreach my $row (@$record_ids) {
    my $id = $row->[0];
    my $type = $GEC->value_for_record_id($id, 'TYPE');
    print "type: $type\n";
}


