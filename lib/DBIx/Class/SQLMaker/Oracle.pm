package # Hide from PAUSE
  DBIx::Class::SQLMaker::Oracle;

use warnings;
use strict;

use base qw( DBIx::Class::SQLMaker );

BEGIN {
  use DBIx::Class::Optional::Dependencies;
  die('The following extra modules are required for Oracle-based Storages ' . DBIx::Class::Optional::Dependencies->req_missing_for ('id_shortener') . "\n" )
    unless DBIx::Class::Optional::Dependencies->req_ok_for ('id_shortener');
}

sub new {
  my $self = shift;
  my %opts = (ref $_[0] eq 'HASH') ? %{$_[0]} : @_;
  push @{$opts{special_ops}}, {
    regex => qr/^prior$/i,
    handler => '_where_field_PRIOR',
  };

  $self->next::method(\%opts);
}

sub _assemble_binds {
  my $self = shift;
  return map { @{ (delete $self->{"${_}_bind"}) || [] } } (qw/select from where oracle_connect_by group having order limit/);
}


sub _parse_rs_attrs {
    my $self = shift;
    my ($rs_attrs) = @_;

    my ($cb_sql, @cb_bind) = $self->_connect_by($rs_attrs);
    push @{$self->{oracle_connect_by_bind}}, @cb_bind;

    my $sql = $self->next::method(@_);

    return "$cb_sql $sql";
}

sub _connect_by {
    my ($self, $attrs) = @_;

    my $sql = '';
    my @bind;

    if ( ref($attrs) eq 'HASH' ) {
        if ( $attrs->{'start_with'} ) {
            my ($ws, @wb) = $self->_recurse_where( $attrs->{'start_with'} );
            $sql .= $self->_sqlcase(' start with ') . $ws;
            push @bind, @wb;
        }
        if ( my $connect_by = $attrs->{'connect_by'} || $attrs->{'connect_by_nocycle'} ) {
            my ($connect_by_sql, @connect_by_sql_bind) = $self->_recurse_where( $connect_by );
            $sql .= sprintf(" %s %s",
                ( $attrs->{'connect_by_nocycle'} ) ? $self->_sqlcase('connect by nocycle')
                    : $self->_sqlcase('connect by'),
                $connect_by_sql,
            );
            push @bind, @connect_by_sql_bind;
        }
        if ( $attrs->{'order_siblings_by'} ) {
            $sql .= $self->_order_siblings_by( $attrs->{'order_siblings_by'} );
        }
    }

    return wantarray ? ($sql, @bind) : $sql;
}

sub _order_siblings_by {
    my ( $self, $arg ) = @_;

    my ( @sql, @bind );
    for my $c ( $self->_order_by_chunks($arg) ) {
        $self->_SWITCH_refkind(
            $c,
            {
                SCALAR   => sub { push @sql, $c },
                ARRAYREF => sub { push @sql, shift @$c; push @bind, @$c },
            }
        );
    }

    my $sql =
      @sql
      ? sprintf( '%s %s', $self->_sqlcase(' order siblings by'), join( ', ', @sql ) )
      : '';

    return wantarray ? ( $sql, @bind ) : $sql;
}

# we need to add a '=' only when PRIOR is used against a column diretly
# i.e. when it is invoked by a special_op callback
sub _where_field_PRIOR {
  my ($self, $lhs, $op, $rhs) = @_;
  my ($sql, @bind) = $self->_recurse_where ($rhs);

  $sql = sprintf ('%s = %s %s ',
    $self->_convert($self->_quote($lhs)),
    $self->_sqlcase ($op),
    $sql
  );

  return ($sql, @bind);
}

# use this codepath to hook all identifiers and mangle them if necessary
# this is invoked regardless of quoting being on or off
sub _quote {
  my ($self, $label) = @_;

  return '' unless defined $label;
  return ${$label} if ref($label) eq 'SCALAR';

  $label =~ s/ ( [^\.]{31,} ) /$self->_shorten_identifier($1)/gxe;

  $self->next::method($label);
}

sub _unqualify_colname {
  my ($self, $fqcn) = @_;

  return $self->_shorten_identifier($self->next::method($fqcn));
}

#
# Oracle has a different INSERT...RETURNING syntax
#

sub _insert_returning {
  my ($self, $options) = @_;

  my $f = $options->{returning};

  my ($f_list, @f_names) = $self->_SWITCH_refkind($f, {
    ARRAYREF => sub {
      (join ', ', map { $self->_quote($_) } @$f),
      @$f
    },
    SCALAR => sub {
      $self->_quote($f),
      $f,
    },
    SCALARREF => sub {
      $$f,
      $$f,
    },
  });

  my $rc_ref = $options->{returning_container}
    or $self->throw_exception('No returning container supplied for IR values');

  @$rc_ref = (undef) x @f_names;

  return (
    ( join (' ',
      $self->_sqlcase(' returning'),
      $f_list,
      $self->_sqlcase('into'),
      join (', ', ('?') x @f_names ),
    )),
    map {
      $self->{bindtype} eq 'columns'
        ? [ $f_names[$_] => \$rc_ref->[$_] ]
        : \$rc_ref->[$_]
    } (0 .. $#f_names),
  );
}

1;
