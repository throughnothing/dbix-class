package DBIx::Class::Storage::DBI::IdentityInsert;

use strict;
use warnings;
use base 'DBIx::Class::Storage::DBI';
use mro 'c3';
use List::Util 'first';
use namespace::clean;

__PACKAGE__->mk_group_accessors(inherited => 'use_identity_update');

__PACKAGE__->mk_group_accessors(simple => qw/
  is_identity_insert is_identity_update _identity_wrap_sql
/);

__PACKAGE__->use_identity_update(1);

=head1 NAME

DBIx::Class::Storage::DBI::IdentityInsert - Storage Component for Sybase ASE and
MSSQL for Identity Inserts

=head1 DESCRIPTION

This is a storage component for Sybase ASE
(L<DBIx::Class::Storage::DBI::Sybase::ASE>) and Microsoft SQL Server
(L<DBIx::Class::Storage::DBI::MSSQL>) to support identity inserts, that is
inserts of explicit values into C<IDENTITY> columns.

This is done by wrapping the C<INSERT> SQL like so:

  SET IDENTITY_INSERT $table ON
  $sql
  SET IDENTITY_INSERT $table OFF

For Sybase ASE C<IDENTITY_UPDATE> is also used.

=head1 METHODS

=head2 is_identity_insert

During the C<insert> through C<_execute> cycle, this accessor indicates whether
an identity insert is being performed. If you set it to C<0> before
C<_prep_for_execute> is invoked, no C<SET> statements will be added.

=head2 is_identity_update

Like L</is_identity_insert> but for C<UPDATE> statements for Sybase ASE.

=cut

sub _table_identity_sql {
  my ($self, $op, $table) = @_;

  my $stmt = 'SET IDENTITY_%s %s %s';
  $table   = $self->sql_maker->_quote($table);

  return [
    sprintf($stmt, $op, $table, 'ON'),
    sprintf($stmt, $op, $table, 'OFF'),
  ];
}

sub insert {
  my $self = shift;
  my ($source, $to_insert) = @_;

  my $is_identity_insert = 0;

  if (first { $_->{is_auto_increment} } values %{ $source->columns_info(
                                                    [keys %$to_insert]
                                                  ) }) {
    $is_identity_insert = 1;
  }

  local $self->{is_identity_insert} = 1 if $is_identity_insert;
  local $self->{_identity_wrap_sql} =
    $self->_table_identity_sql(INSERT => $source->name)
    if $is_identity_insert;

  return $self->next::method(@_);
}

sub update {
  my $self = shift;
  my ($source, $fields) = @_;

  return $self->next::method(@_) unless $self->use_identity_update;

  my $is_identity_update = 0;

  if (first { $_->{is_auto_increment} } values %{ $source->columns_info(
                                                    [keys %$fields]
                                                  ) }) {
    $is_identity_update = 1;
  }

  local $self->{is_identity_update} = 1 if $is_identity_update;
  local $self->{_identity_wrap_sql} =
    $self->_table_identity_sql(UPDATE => $source->name)
    if $is_identity_update;

  return $self->next::method(@_);
}

sub insert_bulk {
  my $self = shift;
  my ($source, $cols) = @_;

  my $is_identity_insert = 0;

  if (first {$_->{is_auto_increment}} values %{$source->columns_info($cols)}){
    $is_identity_insert = 1;
  }

  local $self->{is_identity_insert} = 1 if $is_identity_insert;
  local $self->{_identity_wrap_sql} =
    $self->_table_identity_sql(INSERT => $source->name)
    if $is_identity_insert;

  return $self->next::method(@_);
}

sub _prep_for_execute {
  my $self = shift;
  my ($op) = @_;

  my ($sql, $bind) = $self->next::method (@_);

  if (  ($op eq 'insert' && $self->is_identity_insert)
     || ($op eq 'update' && $self->is_identity_update)) {

    my ($prepend, $append) = @{ $self->_identity_wrap_sql };

    $sql = "${prepend}\n${sql}\n${append}";
  }

  return ($sql, $bind);
}

=head1 AUTHOR

See L<DBIx::Class/AUTHOR> and L<DBIx::Class/CONTRIBUTORS>.

=head1 LICENSE

You may distribute this code under the same terms as Perl itself.

=cut

1;
