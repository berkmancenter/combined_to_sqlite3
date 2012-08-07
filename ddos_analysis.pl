#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Apache::LogRegex;
use Data::Dumper;
use DateTime::Format::HTTP;

use constant {
    HOST =>'%h',
    LOGNAME => '%l',
    VIRTHOST => '%v',
    REMOTE_USER => '%u',
    TIME => '%t',
    REQUEST => '%r',
    STATUS => '%>s',
    BYTES => '%b',
    HTTP_REFERER => '%{Referer}i',
    USER_AGENT => '%{User-Agent}i'
};

my $dbh = DBI->connect("dbi:SQLite:dbname=ddos_info.sqlite3","","",{ RaiseError => 1, AutoCommit => 0 }) or croak($DBI::errstr);
my $lr = Apache::LogRegex->new('%h %v %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"');
my $dt_class = 'DateTime::Format::HTTP';


#38.99.97.155 www.citmedialaw.org - [28/Aug/2010:06:42:46 -0400] "GET / HTTP/1.1" 200 6275 "-" "Mozilla/5.0 (compatible; ScoutJet; +http://www.scoutjet.com/)"
#123.125.71.15 citmedialaw.org - [28/Aug/2010:06:43:28 -0400] "GET / HTTP/1.1" 301 194 "-" "Baiduspider+(+http://www.baidu.com/search/spider.htm)"

$dbh->do('CREATE TABLE IF NOT EXISTS requests(
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    host TEXT,
    virt_host TEXT,
    remote_user TEXT,
    request_time INTEGER, 
    request TEXT,
    status_code TEXT,
    bytes INTEGER,
    referer TEXT, 
    user_agent TEXT 
    )');

my $insert = $dbh->prepare('INSERT INTO requests(
    host,
    virt_host,
    remote_user,
    request_time,
    request,
    status_code,
    bytes,
    referer,
    user_agent) 
values(?,?,?,?,?,?,?,?,?)');

my $i = 1;

while(<>){
    my %l;
    eval{%l = $lr->parse($_)};
    if($@){
        warn 'Unable to parse line: ' . $@;
    }
    print "inserted $i so far. . .\n" if ($i % 1000 == 0);

    $insert->execute(
        $l{(HOST)},
        $l{(VIRTHOST)},
        $l{(REMOTE_USER)},
        apache_to_epoch_time($l{(TIME)}),
        $l{(REQUEST)},
        $l{(STATUS)},
        $l{(BYTES)},
        $l{(HTTP_REFERER)},
        $l{(USER_AGENT)}
    );
    $i++;
}

$insert->finish();

foreach my $column (('host','virt_host','remote_user','request_time','request','status_code','bytes','referer','user_agent')){
    print "Adding $column index\n";
    $dbh->do("CREATE INDEX IF NOT EXISTS requests_${column}s on requests(${column})");
}

$dbh->commit();

$dbh->disconnect();

sub apache_to_epoch_time{
    my $time = shift;
    $time =~ s/\[|\]//g;
    my $dt = $dt_class->parse_datetime($time);
    $dt_class->format_datetime($dt);
    return $dt->epoch();
}

