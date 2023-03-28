from player import PlayerCareer, DataWriter
from push_to_s3 import UploadToS3

if __name__ == "__main__":
    for season_type_var in ('Regular Season', 'Playoffs'):
        curry = PlayerCareer(player_full_name="Stephen Curry", season_type=season_type_var)
        DataWriter(player=curry).write()

    UploadToS3().upload()