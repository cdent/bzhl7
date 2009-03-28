#!/usr/bin/env perl

use strict;
use warnings;

our $DSN = 'DBI:mysql:database=gec';
our $USER = 'cdent';
our $GEC;

use GEC;
$GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);

use YAML;

our @WANTED_TAGS = qw(FINDINGS);

my @FIELD_INFO = @ARGV;


my @all_records;
foreach my $info (@FIELD_INFO) {
    my ($field, $value) = split(':', $info);
    my $record_ids = $GEC->record_ids_for_name($field, $value);
    # XXX could be dupes in here, worry about later
    push(@all_records, @$record_ids);
}

my @missing;
foreach my $tag (@WANTED_TAGS) {
    foreach my $record_id (@all_records) {
        my $value = $GEC->value_for_record_id($record_id, $tag);
        push(@missing, $record_id) unless $value;
    }
}

#print "The following records ids are missing at least one @WANTED_TAGS\n";
print join("\n", @missing);
#print "\n";
