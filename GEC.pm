package GEC;
use strict;
use warnings;
use Class::Field qw(field);
use DBI();
use Data::UUID;
use IO::File;

field 'dbh';
field 'ename';

# Generic Entity Cloud Style Data Handling
# in Perl. Based on conversations with 
# Walt Woolfolk.

my $DSN_DEFAULT = 'DBI:mysql:database=gec';
my $DSN_FILE = '.dsn';

sub ReadDSN {
    my $class = shift;
    my $db_name = shift || 'gec';
    my $dsn = $DSN_DEFAULT;
    if (-r $DSN_FILE) {
        my $fh = new IO::File "< $DSN_FILE" || die "oops: $!";
        $dsn = join('', <$fh>);
        chomp($dsn);
    }
    $dsn =~ s/%db_name%/$db_name/;
    return $dsn;
}

sub new {
    my $class = shift;
    my %input_params = @_;
    my %params = (
        # set defaults
        %input_params,
    );
    my $self = bless {}, $class;
    return $self->_init(\%params);
}

sub _init {
    my $self = shift;
    my $params = shift;

    $self->ename($params->{ename});

    my $dbh = DBI->connect($params->{dsn}, $params->{user}, '', {'RaiseError' => 1});
    $self->dbh($dbh);
    $self->{table} = {};

    return $self;
}

sub create {
    my $self = shift;

    $self->dbh->do("CREATE TABLE IF NOT EXISTS "
            . $self->ename . '_keys'
            . " (keyid VARCHAR(42) PRIMARY KEY, keyname VARCHAR(100))");
    $self->dbh->do("CREATE INDEX IF NOT EXISTS keyname on " . $self->ename . '_keys' 
            . " (keyname)");
    $self->dbh->do("CREATE TABLE IF NOT EXISTS "
            . $self->ename . '_values'
            . " (valueid VARCHAR(42), keyid VARCHAR(42), value TEXT, PRIMARY KEY(valueid, keyid))");
    $self->dbh->do("CREATE INDEX IF NOT EXISTS value on " . $self->ename . '_values'
            . " (value)");
    return $self;
}

# insert a hash into a named table pair
sub put {
    my $self = shift;
    my $data = shift;
    my $id   = shift;

    for my $key (keys(%$data)) {
        my $sth
            = $self->dbh->prepare("SELECT keyid from "
                . $self->_keys_t()
                . " where keyname = ?");
        $sth->execute($key);
        # If keyname is not in the keys table, let's put it in there
        my $uuid;
        if ($sth->rows) {
            my $result = $sth->fetchrow_hashref;
            $uuid = $result->{keyid};
        }
        else {
            $uuid = Data::UUID->new->create_str();
            $self->dbh->do(
                "INSERT INTO " . $self->_keys_t() . " VALUES(?, ?)",
                undef, $uuid, $key);
        }
        # now put the row in values
        $self->dbh->do(
            "REPLACE INTO " . $self->_values_t()  . " VALUES(?, ?, ?)",
            undef, $id, $uuid, $data->{$key});
    }

    return $id;
}

# get an id from a named table pair
sub get {
    my $self = shift;
    my $id   = shift;
    my $data = {};

    my $values_t = $self->_values_t();
    my $keys_t = $self->ename . '_keys';
    my $sth = $self->dbh->prepare(
        "SELECT $values_t.value, $keys_t.keyname FROM $values_t, $keys_t "
            . "WHERE valueid=? AND $keys_t.keyid=$values_t.keyid");
    $sth->execute($id);
    while (my $result = $sth->fetchrow_hashref) {
        $data->{$result->{keyname}} = $result->{value};
    }
    $data->{id} = $id;
    return $data;
}

# get all the entities from a named table pair
# (as a cursor?)
sub all {
    my $self = shift;
    my $values_t = $self->_values_t();
    my $sth = $self->dbh->prepare("SELECT valueid FROM $values_t group by valueid");
    $sth->execute;
    return $sth->fetchall_arrayref;
}

# get the key id for any provide name
sub keyid_for_name {
    my $self = shift;
    my $name = shift;
    my $keys_t = $self->_keys_t();
    my $sth = $self->dbh->prepare("SELECT keyid from $keys_t where keyname=?");
    $sth->execute($name);
    my $results = $sth->fetchrow_arrayref;
    return $results ? $results->[0] : undef;
}

# get the record ids for any keyname
# if value is prepended with ~, a 
# like '%$value%' query will be done
sub record_ids_for_name {
    my $self = shift;
    my $name = shift;
    my $value = shift;
    my $keyid = $self->keyid_for_name($name);
    return [] unless $keyid;
    my $sth;
    if ($value =~ s/^~(.*)/%$1%/) {
        $sth = $self->dbh->prepare("SELECT valueid from " . $self->_values_t() .
            " where keyid=? and value like ?");
    }
    else {
        $sth = $self->dbh->prepare("SELECT valueid from " . $self->_values_t() .
            " where keyid=? and value=?");
    }
    $sth->execute($keyid, $value);
    return [map {$_->[0]} @{$sth->fetchall_arrayref}];
}

sub unique_record {
    my $self = shift;
    my %params = @_;
    my $ids = $self->record_ids_for_name($params{key_name}, $params{key_value});
    my $results = [];
    foreach my $id (@$ids) {
        warn "id: $id\n";
        push(@$results, $self->get($id));
    }

    $results = [sort {$b->{$params{uniquing_field}} cmp $a->{$params{uniquing_field}}} @$results];
    return $results ? $results->[0] :undef;
}

# if we have a record id, and the name of a key we want, get the
# value 
sub value_for_record_id {
    my $self = shift;
    my $id = shift;
    my $name = shift;
    my $keyid = $self->keyid_for_name($name);
    return undef unless $keyid;
    my $sth = $self->dbh->prepare("SELECT value from " . $self->_values_t() .
        " where keyid=? and valueid=?");
    $sth->execute($keyid, $id);
    my $results = $sth->fetchrow_arrayref;
    return $results ? $results->[0] : undef;
}

# get 1 recordid for each instance of a particular key_name.
# That is for everywhere PATIENT_NAME is frank there may
# be many records, but we only return one valueid/recordid.
# We do this so we can then look up other patient information
# rather than record information.
sub records_for_key_name {
    my $self = shift;
    my $key_name = shift;
    return undef unless $key_name;
    my $keyid = $self->keyid_for_name($key_name);
    return undef unless $keyid;
    my $sth = $self->dbh->prepare("SELECT valueid from " . $self->_values_t() .
        " where keyid=? group by value");
    $sth->execute($keyid);
    return $sth->fetchall_arrayref;
}

# return all the valueids associated with values for which the provided string
# is a match.
sub search {
    my $self = shift;
    my $query = shift;
    my $sth = $self->dbh->prepare("SELECT valueid FROM " . $self->_values_t() .
        " WHERE value LIKE ? GROUP BY valueid");
    $sth->execute("%$query%");
    return [map {$_->[0]} @{$sth->fetchall_arrayref}];
}

sub _values_t {
    my $self = shift;
    return $self->ename . '_values';
}

sub _keys_t {
    my $self = shift;
    return $self->ename . '_keys';
}

1;
