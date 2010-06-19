#!/usr/bin/perl

use warnings;
use strict;

use Getopt::Long qw/:config gnu_getopt bundling_override no_ignore_case/;
use FindBin;

use lib "$FindBin::Bin/../t/lib";

my %args;
BEGIN {
  GetOptions (\%args, 'lib|l');
  lib->import ( "$FindBin::Bin/../lib" )
    if $args{lib};
}

use Benchmark qw/timethis cmpthese/;
use DBICTest;

BEGIN {
  package DBICTest::Bench::HRI;
  our $VERSION = 'blah';  # satisfy load_optional_class
  1;
}
use DBIx::Class::ResultClass::HashRefInflator;


#
# So you wrote a new mk_hash implementation which passed all tests (particularly 
# t/68inflate_resultclass_hashrefinflator) and would like to see how it holds up 
# against older versions of the same. Just add your coderef to the HRI::Bench 
# namespace and add a name/ref pair to the %bench_list hash. Happy testing.

my $mk_hash_4761;
$mk_hash_4761 = sub {
    if (ref $_[0] eq 'ARRAY') {     # multi relationship 
        return [ map { $mk_hash_4761->(@$_) || () } (@_) ];
    }
    else {
        my $hash = {
            # the main hash could be an undef if we are processing a skipped-over join 
            $_[0] ? %{$_[0]} : (),

            # the second arg is a hash of arrays for each prefetched relation 
            map
                { $_ => $mk_hash_4761->( @{$_[1]->{$_}} ) }
                ( $_[1] ? (keys %{$_[1]}) : () )
        };

        # if there is at least one defined column consider the resultset real 
        # (and not an emtpy has_many rel containing one empty hashref) 
        for (values %$hash) {
            return $hash if defined $_;
        }

        return undef;
    }
};

# the (incomplete, fails a test) implementation before svn:4760
my $mk_hash_old;
$mk_hash_old = sub {
    my ($me, $rest) = @_;

    # $me is the hashref of cols/data from the immediate resultsource
    # $rest is a deep hashref of all the data from the prefetched
    # related sources.

    # to avoid emtpy has_many rels contain one empty hashref
    return undef if (not keys %$me);

    my $def;

    foreach (values %$me) {
        if (defined $_) {
            $def = 1;
            last;
        }
    }
    return undef unless $def;

    return { %$me,
        map {
          ( $_ =>
             ref($rest->{$_}[0]) eq 'ARRAY'
                 ? [ grep defined, map $mk_hash_old->(@$_), @{$rest->{$_}} ]
                 : $mk_hash_old->( @{$rest->{$_}} )
          )
        } keys %$rest
    };
};


my %bench_list = (
    rev4761 => $mk_hash_4761,
    old_implementation => $mk_hash_old,
);

my $schema = DBICTest->init_schema();

my $test_sub = sub {
    my $rs_hashrefinf = $schema->resultset ('Artist')->search ({}, {
        prefetch => { cds => 'tracks' },
    });
    $rs_hashrefinf->result_class('DBICTest::Bench::HRI');
    my @stuff = $rs_hashrefinf->all;
};


my $results;
for my $b ('__CURRENT__', keys %bench_list) {

    print "Timing $b... ";

    # switch the inflator
    no warnings qw/redefine once/;
    no strict qw/refs/;
    my $cref = $bench_list{$b};
    local *DBICTest::Bench::HRI::inflate_result = $cref
      ? sub { $cref->(@_[2,3]) }
      : sub { DBIx::Class::ResultClass::HashRefInflator->inflate_result (@_[2,3]) }
    ;

    $results->{$b} = timethis (-2, $test_sub);
}
print "\n";
cmpthese ($results);
