from django.test import TestCase
#from django.test.utils import override_settings
from .models import Image, File, Photo, FooFile, FooImage, FooPhoto


class ProxyDeleteImageTest(TestCase):
    '''
    Tests on_delete behaviour for proxy models. Deleting the *proxy*
    instance bubbles through to its non-proxy and *all* referring objects
    are deleted.
    '''

    def setUp(self):
        # Create an Image
        self.test_image = Image()
        self.test_image.save()
        foo_image = FooImage(my_image=self.test_image)
        foo_image.save()

        # Get the Image instance as a File
        test_file = File.objects.get(pk=self.test_image.pk)
        foo_file = FooFile(my_file=test_file)
        foo_file.save()

    #@override_settings(DEBUG=True)
    def test_delete(self):
        self.assertTrue(Image.objects.all().delete() is None)
        # An Image deletion == File deletion
        self.assertEqual(len(Image.objects.all()), 0)
        self.assertEqual(len(File.objects.all()), 0)
        # The Image deletion cascaded and *all* references to it are deleted.
        self.assertEqual(len(FooImage.objects.all()), 0)
        self.assertEqual(len(FooFile.objects.all()), 0)
        #from django.db import connection
        #for q in connection.queries:
        #    print q


class ProxyDeletePhotoTest(ProxyDeleteImageTest):
    '''
    Tests on_delete behaviour for proxy-of-proxy models. Deleting the *proxy*
    instance should bubble through to its proxy and non-proxy variants.
    Deleting *all* referring objects. For some reason it seems that the 
    intermediate proxy model isn't cleaned up.
    '''

    def setUp(self):
        # Create the Image, FooImage and FooFile instances
        super(ProxyDeletePhotoTest, self).setUp()
        # Get the Image as a Photo
        test_photo = Photo.objects.get(pk=self.test_image.pk)
        foo_photo = FooPhoto(my_photo=test_photo)
        foo_photo.save()


    #@override_settings(DEBUG=True)
    def test_delete(self):
        self.assertTrue(Photo.objects.all().delete() is None)
        # A Photo deletion == Image deletion == File deletion
        self.assertEqual(len(Photo.objects.all()), 0)
        self.assertEqual(len(Image.objects.all()), 0)
        self.assertEqual(len(File.objects.all()), 0)
        # The Photo deletion should have cascaded and deleted *all*
        # references to it.
        self.assertEqual(len(FooPhoto.objects.all()), 0)
        self.assertEqual(len(FooFile.objects.all()), 0)
        self.assertEqual(len(FooImage.objects.all()), 0)
        #from django.db import connection
        #for q in connection.queries:
        #    print q
        #print len(connection.queries)


class ProxyDeleteFileTest(ProxyDeleteImageTest):
    '''
    Tests on_delete cascade behaviour for proxy models. Deleting the
    *non-proxy* instance of a model should somehow notify it's proxy.
    Currently it doesn't, making this test fail.
    '''

    def test_delete(self):
        # This should *not* raise an IntegrityError on databases that support 
        # FK constraints.
        self.assertTrue(File.objects.all().delete() is None)
        # A File deletion == Image deletion
        self.assertEqual(len(File.objects.all()), 0)
        self.assertEqual(len(Image.objects.all()), 0)
        # The File deletion should have cascaded and deleted *all* references
        # to it.
        self.assertEqual(len(FooFile.objects.all()), 0)
        self.assertEqual(len(FooImage.objects.all()), 0)
