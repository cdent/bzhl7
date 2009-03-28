#!/usr/bin/env perl

use strict;
use warnings;

our %opt;
our $DSN = 'DBI:mysql:database=gec';
our $USER = 'cdent';
our $GEC;

our $OBX_MATCH = qr{^\s*((?:[[:upper:]]\w+\s*)+):(.*$)};

use Getopt::Std;
getopts('nx', \%opt);  # -n to not put things in database
                      # -x to do only one loop

use GEC;
unless ($opt{n}) {
    $GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);
    $GEC->create();
}

my $pid;
my $orc;
my $obr;
my $obx;
my $raw_hl7;


# HL7 parsing rules for PID, ORC and OBR lines.
our $HL7 = {
    'PID' => {
        3 => 'UNIT_NUMBER',
        5 => 'PATIENT',
        18 => 'ACCOUNT_NUMBER',
    },
    'ORC' => {
        12 => 'ORDERING_PROVIDER',
    },
    'OBR' => {
        4  => 'SERVICE_ID',
        7  => 'OBSERVATION_START',
        8  => 'OBSERVATION_END',
        32 => 'PRINCIPAL_RESULT_INTERPRETER',
        35 => 'TRANSCRIPTIONIST',
    }
};

# This loop only works if and only if
# 
# The file has been processed with s/\r/\n/ (dos2unix)
#
# AND
#
# Each record:
# # Starts with MSH
# # Ends with an empty line (including the final record)
# # Has a line begining with PID that contains record
# # # information (such as name of patient).
# # Has multiple lines that begin with OBX.

# run like this (once the data file has been dos2unix
# process has been done):
#
#    ./slice_dice.pl < <data_file>
#
# We look for the identifying markers of each line
# and push the data into a hash that we reuse.
while (<>) {
    s/\s+$//;    # a simple chomp doesn't work here
    /^MSH/ && do {
        # new record, destroy exiting data
        clear_data();
    };
    /^.*$/ && do {
        $raw_hl7 .= "$_\n";
    };
    # XXX these next three match could become one
    /^PID/ && do {
        my @splits = split('\|', $_);
        $pid = \@splits;
    };
    /^ORC/ && do {
        my @splits = split('\|', $_);
        $orc = \@splits;
    };
    /^OBR/ && do {
        my @splits = split('\|', $_);
        $obr = \@splits;
    };
    /^OBX/ && do {
        my @splits = split('\|', $_);
        my $obx_line = $splits[5] || ' ';
        if ($obx_line) {
            $obx_line =~ s/^\s*//;
            $obx_line =~ s/\s*$//;
            push(@$obx, $obx_line);
        }
    };
    /^$/ && do {
        # we now have a complete record
        # so...
        if (@$pid && @$obr && @$orc && @$obx) {
            handle_rule(raw_hl7 => $raw_hl7, pid => $pid, orc => $orc, obr => $obr, obx => $obx);
            clear_data();
        }
    };
}

sub clear_data {
    $pid = [];
    $obr = [];
    $orc = [];
    $obx = [];
    $raw_hl7 = '';
}

sub handle_rule {
    my %params = @_;

    my $status = $params{obr}->[25];
    # if status is not S we are not "signed"
    # so don't want it
    return unless ($status eq 'S');

    # figure out what kind of record we have
    my $type = $params{obr}->[21];

    print '#' x 24, "\n";

    # get our OBX start index
    my $start_index = 12;

    # this is where we will store our data for this record
    my $gathered_data = {TYPE => $type};
    $gathered_data->{raw_hl7} = $params{raw_hl7};
    $gathered_data->{OBX} = join("\n", @{$params{obx}});

    # handle parsing out stuff from PID, ORC and OBR
    foreach my $line ('PID', 'ORC', 'OBR') {
        my $line_rules = $HL7->{$line};
        foreach my $index (keys(%$line_rules)) {
            my $data = $params{lc($line)}->[$index];
            my $name = $line_rules->{$index};
            $gathered_data->{$name} = $data;
        }
    }

    # parse the OBX body by inference
    my $body_key;
    foreach
        my $obx (@{ $params{obx} }[ $start_index + 1 .. $#{ $params{obx} } ]) {

        # if we have some content on the line and it looks like a key of
        # some sort, parse the key and get the data following.
        #if ($obx and ($obx =~ /^\s*((?:[[:upper:]]\w+\s*)+):(.*$)/)) {
        if ($obx and ($obx =~ $OBX_MATCH)) {
            my $section = $1;
            my $extra   = $2;
            $body_key = $section;
            $body_key =~ s/ /_/g;
            if ($extra) {
                $gathered_data->{$body_key} = $extra;
            }
            else {
                $gathered_data->{$body_key} = '';
            }

          # otherwise we have some body info, make it part of the value of the
          # current key
        }
        elsif ($obx) {
            if ($body_key) {
                $gathered_data->{$body_key} .= "\n" . $obx;
            }
        }
    }

    if ($opt{n}) {
        # Print out a serialization of the data in a semi-readable form.
        use YAML;
        print Dump($gathered_data);
    }
    else {
        # put it in the db
        my $id = Data::UUID->new->create_str();
        $id = $GEC->put($gathered_data, $id);
        print "PUT $type at $id\n";
    }

    if ($opt{x}) {
        exit;
    }
}
