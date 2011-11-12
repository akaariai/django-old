from django.db import models

class File(models.Model):
    pass

class Image(File):
    class Meta:
        proxy = True

class Photo(Image):
    class Meta:
        proxy = True

class FooImage(models.Model):
    my_image = models.ForeignKey(Image)
    
class FooFile(models.Model):
    my_file = models.ForeignKey(File)

class FooPhoto(models.Model):
    my_photo = models.ForeignKey(Photo)

