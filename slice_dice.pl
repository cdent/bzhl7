#!/usr/bin/env perl

use strict;
use warnings;

my $pid;
my $post_pid;
my $obrid;
my $name;
my $obx;
my $obr;

# These are the OBX parsing rules for what might be
# called the header at the top of the section. The
# individual chunks of data that are not narrative,
# but are semi-structure data. We use the rules to
# parse that data into a hash. The narrative data
# is processed by inference after the RULES are applied.
#
# Ideally these rules would/will be kept in other 
# files in a less noisy and easier to edit format
# so we can just create rule files and apply them 
# as needed. We'll get to that eventually.
#
# Similarly, these rules should allow us to remove
# a fair amount of redundancy between the various
# subroutines that handle the OBX data.
#
# At the top level we have the OBR code, and then
# each rule has the following fields:
#
# index: the line in the OBX where this rule will
#        be applied
# pattern: the regular expression of the rule, 
#          resulting in a list of matches
# keys: the data keys to which the matches are
#       are assigned
# 
# It may be possible to programmatically generate
# the keys, but this functionality is here for
# the sake of flexibility and demonstrating the
# possibility.
our $RULES = {
     'MRAD' => [
     {
         'index' => [6,7],
         'pattern' => qr/Patient:\s+Sex:\s+Account Number:.*\n\s*(\w+.*)\s{2,}\s+(\S+)\s+(\S+)\s*$/i,
         'keys' => [qw(PATIENT SEX ACCT)],
     },
     {
         'index' => [8,9],
         'pattern' => qr/Ordering Physician:\s+Status:\s+Location:\s+Unit Number:.*\n\s*(\w+.*)\s{2,}\s+(\w+.*)\s{2,}\s+(\S+)\s+(\S+)\s*$/i,
         'keys' => [qw(ORDERING_P STATUS LOCATION UNIT_NUMB)],
     },
     {
         'index' => [10,11],
         'pattern' => qr/Attending Physician:\s+Date of Birth:\s+Age:\s+Date of Exam:.*\n\s*(\w+.*)\s{2,}\s+(\S+)\s+(\S+)\s+(\S+)\s*$/i,
         'keys' => [qw(ATTENDING_P DOB AGE DATE_OF_EXAM)],
     },
     ],
    'MCTH' => [
    {
        'index' => [3],
        'pattern' => qr{MR #:\s*(\S+)\s+ACCT #:\s*(\S+)}i,
        'keys' => [qw(MR ACCT)],
    },
    {
        'index' => [4],
        'pattern' => qr{Adm Date:\s*(\S+)\s+Room:\s*(\S+)}i,
        'keys' => [qw(ADM_DATE ROOM)],
    },
    {
        'index' => [5],
        # XXX this pattern eats too much white space. Not sure why.
        'pattern' => qr{Patient Type:\s*(.*)?\s{2}\s*DOB:\s*(\S+)}i,
        'keys' => [qw(PATIENT_TYPE DOB)],
    },
    {
        'index' => [6],
        'pattern' => qr{Physician:\s*(\S+.*)}i,
        'keys' => [qw(PHYSICIAN)],
    },
    {
        'index' => [7],
        'pattern' => qr{EMR ID:\s*(\S+.*)}i,
        'keys' => [qw(EMR_ID)],
    },
    {
        'index' => [8],
        'pattern' => qr{AGE:\s*(\S+)\s*DOS:\s*(\S+)}i,
        'keys' => [qw(AGE DOS)],
    },
    ],
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
        undef $pid;
        undef $post_pid;
        undef $obrid;
        undef $obr;
        undef $name;
        $obx = [];
    };
    /^PID/ && do {

        # get the pid line
        my @splits = split('\|', $_);
        $pid      = $splits[3];
        $post_pid = $splits[-1];
        $name     = $splits[5];
        $name =~ s/\^/ /g;
    };
    /^OBR/ && do {
        my @splits = split('\|', $_);
        $obrid = $splits[2];
        $obr   = $splits[21];
    };
    /^OBX/ && do {
        my @splits = split('\|', $_);
        my $obx_line = $splits[5];
        if ($obx_line) {
            $obx_line =~ s/^\s*//;
            $obx_line =~ s/\s*$//;
            push(@$obx, $obx_line);
        }
    };
    /^$/ && do {

        # we now have a complete record
        # call a subroutine based on the obr code, if one doesn't
        # exist, we just move on
        no strict 'refs';
        #&$obr(
        handle_rule(
            pid   => $pid,   name => $name, post_pid => $post_pid,
            obrid => $obrid, obx  => $obx,  obr      => $obr
        );

        #exit;
    };
}

sub handle_rule {
    my %inparams = @_;
    my %params   = (

        # defaults
        %inparams,
    );

    return unless $RULES->{$params{obr}};

    # output the general info we got from the msh and nearby lines
    print "filename: ", $params{pid}, '_', $params{post_pid}, '_',
        $params{obrid}, '.html', "\n";
    print "name: $params{name}\n";
    print "obr: $params{obr}\n";

    # store the data for this record in a hash
    my $obx_data = {};

    # parse the OBX header info by RULES
    # max index is the line where we think the headers have ended
    # and the body begins. We calculate it while processing the
    # headers.
    my $max_index = 0;
    foreach my $rule (@{ $RULES->{$params{obr}} }) {
        my @index = @{$rule->{index}};
        #print STDERR "@index\n";
        my $index_max = $index[-1];
        $max_index = $max_index > $index_max ? $max_index : $index_max;
        my $line = join("\n", @{$params{obx}}[@index]);
        #print STDERR "@index#$line\n#";
        @$obx_data{ @{ $rule->{keys} } } = ($line =~ $rule->{pattern});
    }

    # parse the OBX body by inference
    my $body_key;
    foreach
        my $obx (@{ $params{obx} }[ $max_index + 1 .. $#{ $params{obx} } ]) {

        # if we have some content on the line and it looks like a key of
        # some sort, parse the key and get the data following.
        if (
            $obx
            and (  $obx =~ /^\s*([[:upper:] ]+):\s+(.*$)/
                or $obx =~ /^\s*([[:upper:] ]+)(\s*$)/)
            ) {
            my $section = $1;
            my $extra   = $2;
            $body_key = $section;
            $body_key =~ s/ /_/g;
            if ($extra) {
                $obx_data->{$body_key} = $extra;
            }
            else {
                $obx_data->{$body_key} = '';
            }

          # otherwise we have some body info, make it part of the value of the
          # key
        }
        elsif ($obx) {
            if ($body_key) {
                $obx_data->{$body_key} .= "\n" . $obx;
            }
        }
    }

    # Print out a serialization of the data in a semi-readable form.
    use YAML;
    print Dump($obx_data);

    # XXX if you want this to run for just one record,
    # uncomment the following line
    #exit;
}
