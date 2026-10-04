use strict;
use warnings;
use Storable qw(fd_retrieve nstore);
use File::Path qw(make_path);

# Keep real biological context while injecting separately labelled synthetic
# known variants in prepare.py. Run inside the pinned VEP image so the native
# Storable ABI and its object classes match the release-116 cache.
my ($source, $destination) = @ARGV;
die "usage: slice_native.pl SOURCE DESTINATION\n" unless $destination;
make_path("$destination/1");
$Storable::canonical = 1;
for my $region ('1-1000000', '1000001-2000000') {
    for my $suffix ('', '_reg') {
        my $name = "$region$suffix.gz";
        open(my $input, '-|', 'gzip', '-dc', "$source/1/$name") or die $!;
        my $cache = fd_retrieve($input);
        close($input) or die "gzip failed for $name";
        my $overlaps = sub {
            my ($feature) = @_;
            return $feature->{start} <= 1005001 && $feature->{end} >= 995000;
        };
        if ($suffix eq '') {
            $cache->{'1'} = [grep { $overlaps->($_) } @{$cache->{'1'}}];
            print "$name transcripts ", scalar(@{$cache->{'1'}}), "\n";
        } else {
            for my $kind (keys %{$cache->{'1'}}) {
                $cache->{'1'}{$kind} = [grep { $overlaps->($_) } @{$cache->{'1'}{$kind}}];
                print "$name $kind ", scalar(@{$cache->{'1'}{$kind}}), "\n";
            }
        }
        nstore($cache, "$destination/1/$region$suffix.storable");
    }
}
