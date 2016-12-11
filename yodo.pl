#!/usr/bin/perl

use strict;
use warnings;

use utf8;
use Encode qw/encode_utf8 decode/;
use DBI;
use Text::CSV::Encoded;
use Time::Piece;

use Data::Dumper;


my $csvname = shift;
my $dbname = 'test.db';

my @main_fields   = qw/date shop card payment installment paymonth price1 price2/;
my @option_fields = qw/id filename registration_date update_state update_date/;


open my $fh,'<',$csvname or die "$!";
my $csv = Text::CSV::Encoded->new({
    encoding_in => 'shiftjis',
    encoding_out => 'UTF-8'
    });

###ISO 8601形式の今の時間を用意する
my $t = localtime(time);
my $timezone = sprintf "+%02d:%02d",$t->tzoffset / 3600 ,($t->tzoffset % 3600) / 60;
my $date = $t->datetime.$timezone; #ISO 8601 style


#データを配列に格納
my @data;
my $id = 0; #idに相当するものがないので自分で作る
while(my $columns = $csv->getline($fh)){
    my $eles;
#    print Dumper $columns;

    @$eles{@main_fields} = @$columns;
    @$eles{@option_fields} = ($id,$csvname,$date,0,$date);

    $eles->{'paymonth'} =~ s/\'//; #シングルクォーテーションを取り除いておく
                                   #個々のフィールドに対する整形処理を書く
    
    push @data,$eles;
    $id++;
}
$csv->eof;



#配列に格納したデータをデータベースに格納
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbname");

#yodoというtableを作成する
 my $fieldlist = join ',',(@main_fields,@option_fields);
#print encode_utf8 $fieldlist."\n";

$dbh->do("create table if not exists yodo ($fieldlist,primary key(id,date))");

foreach my $ele(@data){
    
    my  @value = @{$ele}{(@main_fields,@option_fields)}; #配列のキー順序の通りにエントリを抽出
    @value    = map{"\'$_\'"}@value;                  #クオーテーションで囲む
    my $valuelist = join ',',@value;

    #同じid,dateのものが無いか確認
    my $stmt = "select * from yodo where id = \'$ele->{'id'}\' and date = \'$ele->{'date'}\'";
#    print $stmt."\n";
    my $sth  = $dbh -> prepare($stmt);
    $sth -> execute;

    next if($sth->fetchrow_arrayref);

    #insert
    #    print encode_utf8 $valuelist."\n";
    #    print encode_utf8 "insert into yodo($fieldlist) values($valuelist)\n";
    $dbh->do("insert into yodo($fieldlist) values($valuelist)");
}

$dbh->disconnect;



sub dump_data{

    my @data = shift;
    foreach my $line(@data){
        foreach my $key (keys %$line){
            print encode_utf8($line->{$key});
            print "\t";
        }
        print "\n";
    }
    
}
#print Dumper @data;

