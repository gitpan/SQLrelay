package DBD::SQLRelay;
 
use strict;
use vars qw($err $errstr $sqlstate $drh);
$VERSION = 0.1;
use Firstworks::SQLRClient;
  
$err = 0;             # holds error code   for DBI::err
$errstr = "";         # holds error string for DBI::errstr
$sqlstate = "";       # holds SQL state for    DBI::state
  
$drh = undef;         # holds driver handle once initialized

sub driver {
 return $drh if $drh;        # already created - return same one
 my($class, $attr) = @_;
 $class .= "::dr";
  # not a 'my' since we use it above to prevent multiple drivers
 $drh = DBI::_new_drh($class, {
 'Name'    => 'SQLRelay',
 'Version' => 0,
 'Err'     => \$DBD::SQLRelay::err,
 'Errstr'  => \$DBD::SQLRelay::errstr,
 'State'   => \$DBD::SQLRelay::state,
 'Attribution' => 'DBD::SQLRelay by Dmitry Ovsyanko',
 'dbhs'    => {}});
 return $drh;
}
  
package DBD::SQLRelay::dr; # ====== DRIVER ======
  
$DBD::SQLRelay::dr::imp_data_size = 0;
  
sub connect {
 my($drh, $dbname, $user, $auth, $attr)= @_;
 my $dbh = DBI::_new_dbh($drh, {
  'Name' => $dbname,
  'USER' => $user,
  'CURRENT_USER' => $user});
 my $var;
 my $val;
 my %dsn;
 foreach $var (split(/;/, $dbname)) {
  if ($var =~ /(.*?)=(.*)/) {
   $var = $1;
   $val = $2;
   $dsn{$var} = $val;
   $dbh->STORE($var, $val)}}
 
 my $host      = $dsn{host}      || 'localhost';
 my $port      = $dsn{port}      || 8000;
 my $retrytime = $dsn{retrytime} || 0;
 my $tries     = $dsn{tries}     || 1;
 my $debug     = $dsn{debug}     || 0;
 
 my $connection = Firstworks::SQLRClient -> new ($host, $port, $user, $auth, $retrytime, $tries);

 $connection -> debugOn() if $debug;
 
# print "\$connection = $connection\n";
 
 $dbh -> STORE ('driver_handle',     $drh);
 $dbh -> STORE ('driver_connection', $connection);
 $drh -> {dbhs} -> {$dbh} = 1;

 unless ($dbh -> ping) {
  return $dbh -> DBI::set_err(-1, $connection -> errorMessage);
#  $dbh -> disconnect;
#  return $dbh
  };

 $dbh;
}

sub disconnect_all {
 my ($drh) = @_;
 foreach (keys %{$drh -> {dbhs}}) {
  my $dbh = $drh -> {dbhs} -> {$_};
  next unless ref $dbh;
  $dbh -> disconnect};
 return 1 };
  
package DBD::SQLRelay::db; # ====== DATABASE ======
  
$DBD::SQLRelay::db::imp_data_size = 0;
 
sub ping {
 my ($dbh) = @_;
 my $c = $dbh -> FETCH ('driver_connection');
 $c -> sendQuery('select 1');
 return 0 unless $c -> rowCount() == 1;
 return 0 unless $c -> colCount() == 1;
 return 0 unless $c -> getFieldByIndex(0, 0) == 1;
 return 1;
};
 
sub prepare {
 my($dbh, $statement, @attribs)= @_;
  
 my $sth = DBI::_new_sth($dbh, {'Statement' => $statement});
 $sth->STORE('driver_params', []);
 $sth->STORE('driver_dbh', $dbh);
 $sth->STORE('driver_is_select', ($statement =~ /^\s*select/i) );
 $sth->STORE('NUM_OF_PARAMS', ($statement =~ tr/?//));
 $sth->STORE('driver_connection', $dbh -> FETCH ('driver_connection'));

 $sth
}

sub disconnect {
 my ($dbh) = @_;
 $dbh -> FETCH ('driver_connection') -> sendEndOfSession;
 delete $dbh -> FETCH ('driver_handle') -> {$dbh};
};
  
sub commit {
 my($dbh) = @_;
 if ($dbh -> FETCH ('driver_AutoCommit')) {
   if ($dbh -> FETCH ('Warn')) {
    warn("Commit ineffective while AutoCommit is on")}}
 return $dbh -> do ('commit'); 
}

sub rollback {
 my($dbh) = @_;
 if ($dbh -> FETCH ('driver_AutoCommit')) {
   if ($dbh -> FETCH ('Warn')) {
    warn("Rollback ineffective while AutoCommit is on")}}
 return $dbh -> do ('rollback'); 
}

sub STORE {
 my($dbh, $attr, $val) = @_;
 if ($attr eq 'AutoCommit') {
  $dbh -> {driver_AutoCommit} = $val;
  return 1}; 
 if ($attr =~ /^driver_/) {
  $dbh->{$attr} = $val; # Yes, we are allowed to do this,
  return 1;             # but only for our private attributes
  }
 $dbh->SUPER::STORE($attr, $val);
}
  
sub FETCH {
 my($dbh, $attr) = @_;
 if ($attr eq 'AutoCommit') {return $dbh -> {driver_AutoCommit}}; 
 if ($attr =~ /^driver_/) {
 return $dbh->{$attr}; # Yes, we are allowed to do this,
 }
  # Else pass up to DBI to handle
 $dbh->SUPER::FETCH($attr);
}
          
package DBD::SQLRelay::st; #===============  STATEMENT  =============
  
$DBD::SQLRelay::st::imp_data_size = 0;
  
sub bind_param {
 my($sth, $pNum, $val, $attr) = @_;
 my $type = (ref $attr) ? $attr->{TYPE} : $attr;
 if ($type) {
  my $dbh = $sth->{Database};
  $val = $dbh->quote($sth, $type)}
 my $params = $sth->FETCH('driver_params');
 $params->[$pNum-1] = $val;
 1;
}
 
sub execute {
 my($sth, @bind_values) = @_;
 my $params = (@bind_values) ? \@bind_values : $sth->FETCH('driver_params');
 my $numParam = $sth->FETCH('NUM_OF_PARAMS');
 if (@$params != $numParam) { die ("execute() with ".@$params." param(s) called, but $numParam needed") };
 my $statement = $sth->{'Statement'};
 
 for (my $i = 0;  $i < $numParam;  $i++) {
  $statement =~ s/\?/$params->[$i]/e}

 my $connection = $sth -> FETCH ('driver_connection');
 $connection -> sendQuery ($statement);
 $connection -> sendQuery ('commit') if $sth -> FETCH ('driver_dbh') -> FETCH ('driver_AutoCommit') and not ($sth -> FETCH ('driver_is_select'));
  
 my $colcount = $connection -> colCount();
 my $rowcount = $connection -> rowCount();
 my @colnames = map {$connection -> getColumnNameByIndex($_)} (0..$colcount - 1);
 $sth -> STORE ('driver_NUM_OF_ROWS',   $rowcount);
 $sth -> STORE ('NUM_OF_FIELDS', $colcount) unless $sth -> FETCH ('NUM_OF_FIELDS');
# $sth -> STORE ('NAME', \@colnames);
 $sth -> {NAME} = \@colnames;
 $sth -> STORE ('driver_FETCHED_ROWS', 0);
 $rowcount || '0E0';
}
  
sub fetchrow_arrayref {
 my($sth) = @_;
 my $connection = $sth -> FETCH ('driver_connection');
 my $fetched_rows = $sth -> FETCH ('driver_FETCHED_ROWS');
 if ($fetched_rows == $sth -> FETCH ('driver_NUM_OF_ROWS')) {
  $sth -> finish;
  return undef};  
 my @row = map {$connection -> getFieldByIndex($fetched_rows, $_)} (0..$sth -> FETCH ('NUM_OF_FIELDS') - 1);
 $sth -> STORE ('driver_FETCHED_ROWS', $fetched_rows + 1);
 if ($sth->FETCH('ChopBlanks')) { map { $_ =~ s/\s+$//; } @row}
 return $sth->_set_fbav(\@row);
}

*fetch = \&fetchrow_arrayref; # required alias for fetchrow_arrayref
 
sub rows { 
 my($sth) = @_; 
 $sth->FETCH('driver_NUM_OF_ROWS') 
}

sub finish {
 my($sth) = @_; 
 $sth->SUPER::finish;
}        

sub STORE {
 my($sth, $attr, $val) = @_;
 if ($attr =~ /^driver_/) {
  $sth->{$attr} = $val; # Yes, we are allowed to do this,
  return 1;             # but only for our private attributes
  }
  $sth->SUPER::STORE($attr, $val);
}
  
sub FETCH {
 my($sth, $attr) = @_;
 if ($attr =~ /^driver_/) {
 return $sth->{$attr}; # Yes, we are allowed to do this,
 }
  # Else pass up to DBI to handle
 $sth->SUPER::FETCH($attr);
}

1;
__END__
#

=head1 NAME

DBD::SQLRelay - perl DBI driver for SQL Relay 

=head1 SYNOPSIS

use DBD::SQLRelay;

my $dbh = DBI -> connect ('dbi:SQLRelay:$dsn', $login, $password);

=head1 DESCRIPTION

This module is a pure-Perl DBI binding to SQL Relay's native API. 
Connection string consists of following parts:

=item B<host=...>      default: I<localhost> --- hostname of SQL Relay server;

=item B<port=...>      default: I<8000>      --- port number that SQL Relay server listens on;

=item B<tries=...>     default: I<1>         --- how much times do we try to connect;

=item B<retrytime=...> default: I<0>         --- time (in seconds) between connect attempts;

=item B<debug=...>     default: I<0>         --- set it to 1 if you want to get some debug messages in stdout;

Once connected, DB handler works as usual (see L<DBI>). 

Don't ever try to share one SQLRelay connect by multiple scripts, for example, if you use 
Apache mod_perl. Every $dbh holds one of server connections, so call disconnect() directly
at the end of every script and don't use Apache::DBI or SQLRelay will be deadlocked.

If you use L<HTML::Mason>, your handler.pl sould look like this:

  ...

     {
       package HTML::Mason::Commands;
       use DBI;
       use vars qw($db);  
     }
 
  ...

     sub handler {
       
       $HTML::Mason::Commands::dbh = DBI -> connect (...);
       
       my $status = $ah -> handle_request (...);
     
       $HTML::Mason::Commands::dbh -> disconnect;
       
       return $status;
              
     }
     


=head1 AUTHOR

D. E. Ovsyanko, do@mobile.ru

=head1 SEE ALSO

http://www.firstworks.com

=cut         