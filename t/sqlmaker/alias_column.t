use strict;
use warnings;

use Test::More;

use lib qw(t/lib);
use DBIC::SqlMakerTest;


use_ok('DBICTest');
use_ok('DBIC::DebugObj');
my $schema = DBICTest->init_schema();

my ($sql, @bind);
$schema->storage->debugobj(DBIC::DebugObj->new(\$sql, \@bind));
$schema->storage->debug(1);

my $rs;

$rs = $schema->resultset('BadNames1')->search({
   'me.good_name' => 2001,
});

eval { $rs->all };

is_same_sql_bind(
  $sql, \@bind,
  "SELECT me.id, me.stupid_name FROM bad_names_1 me WHERE ( me.stupid_name = ? )", ["'2001'"],
  'got correct SQL for count query with bracket quoting'
);

done_testing;
