#!/usr/bin/env perl

use strict;
use warnings;

use Benchmark qw/cmpthese/;
use FindBin;
use lib "$FindBin::Bin/../../t/lib";
use lib "$FindBin::Bin/../../lib";
use DBICTest::Schema;
use DBIx::Class::ResultClass::HashRefInflator;  # older dbic didn't load it

printf "Benchmarking DBIC version %s\n", DBIx::Class->VERSION;

my $schema = DBICTest::Schema->connect ('dbi:SQLite::memory:');
$schema->deploy;

my $rs = $schema->resultset ('Artist');

my $hri_rs = $rs->search ({}, { result_class => 'DBIx::Class::ResultClass::HashRefInflator' } );

#DB::enable_profile();
#my @foo = $hri_rs->all;
#DB::disable_profile();
#exit;

my $dbh = $schema->storage->dbh;
my $sql = sprintf ('SELECT %s FROM %s %s',
  join (',', @{$rs->_resolved_attrs->{select}} ),
  $rs->result_source->name,
  $rs->_resolved_attrs->{alias},
);

$rs->populate ([ map { { name => "Art_$_"} } (0 .. 49) ]);
print "\nRetrieval of 100 rows\n";
bench();


$rs->populate ([ map { { name => "Art_$_"} } (100 .. 9999) ]);
print "\nRetrieval of 10,000 rows\n";
bench();

sub bench {
  cmpthese(-3, {
    Cursor => sub { my @r = $rs->cursor->all },
    HRI => sub { my @r = $hri_rs->all },
    RowObj => sub { my @r = $rs->all },
    DBI_AoH => sub { my @r = @{ $dbh->selectall_arrayref ($sql, { Slice => {} }) } },
    DBI_AoA=> sub { my @r = @{ $dbh->selectall_arrayref ($sql) } },
  });
}
