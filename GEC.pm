package GEC;
use strict;
use warnings;
use Class::Field qw(field);
use DBI();
use Data::UUID;

field 'dbh';
field 'ename';

# Generic Entity Cloud Style Data Handling
# in Perl. Based on conversations with 
# Walt Woolfolk.

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
    $self->dbh->do("CREATE TABLE IF NOT EXISTS "
            . $self->ename . '_values'
            . " (valueid VARCHAR(42), keyid VARCHAR(42), value TEXT, PRIMARY KEY(valueid, keyid))");
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
                . $self->ename . '_keys'
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
                "INSERT INTO " . $self->ename . '_keys' . " VALUES(?, ?)",
                undef, $uuid, $key);
        }
        # now put the row in values
        $self->dbh->do(
            "REPLACE INTO " . $self->ename . '_values' . " VALUES(?, ?, ?)",
            undef, $id, $uuid, $data->{$key});
    }

    return $id;
}

# get an id from a named table pair
sub get {
    my $self = shift;
    my $id   = shift;
    my $data = {};

    my $values_t = $self->ename . '_values';
    my $keys_t = $self->ename . '_keys';
    my $sth = $self->dbh->prepare(
        "SELECT $values_t.value, $keys_t.keyname FROM $values_t, $keys_t "
            . "WHERE valueid=? AND $keys_t.keyid=$values_t.keyid");
    $sth->execute($id);
    while (my $result = $sth->fetchrow_hashref) {
        $data->{$result->{keyname}} = $result->{value};
    }
    return $data;
}

# get all the entities from a named table pair
# (as a cursor?)
sub all {
    my $self = shift;
    my $values_t = $self->ename . '_values';
    my $sth = $self->dbh->prepare("SELECT valueid FROM $values_t group by valueid");
    $sth->execute;
    return $sth->fetchall_arrayref;
}

1;
