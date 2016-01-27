# Worker

[![Documentation Status](https://readthedocs.org/projects/moira/badge/?version=latest)](http://moira.readthedocs.org/en/latest/?badge=latest) [![Build Status](https://travis-ci.org/moira-alert/worker.svg?branch=master)](https://travis-ci.org/moira-alert/worker) [![Coverage Status](https://coveralls.io/repos/moira-alert/worker/badge.svg?branch=master&service=github)](https://coveralls.io/github/moira-alert/worker?branch=master) [![Code Health](https://landscape.io/github/moira-alert/worker/master/landscape.svg?style=flat)](https://landscape.io/github/moira-alert/worker/master) [![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/moira-alert/moira?utm_source=badge&utm_medium=badge&utm_campaign=badge)


Code in this repository is a part of Moira monitoring application. Other parts are [Web][web], [Cache][cache] and [Notifier][notifier].

Worker is responsible for processing incoming metric stream from Cache, checking metric state and generating events for Notifier.

Documentation for the entire Moira project is available on [Read the Docs][readthedocs] site.

If you have any questions, you can ask us on [Gitter][gitter].

## Update from 1.0.x to 1.1

Version 1.1 change metric store values format.

1. Stop moira-checker, moira-api v1.0.x
2. Update moira-cache to version 1.1 and run new version
3. Run moira-update. It will convert old value to new format
4. Update moira-checker, moira-api to version 1.1 and run new version

## Thanks

![SKB Kontur](https://kontur.ru/theme/ver-1652188951/common/images/logo_english.png)

Moira was originally developed and is supported by [SKB Kontur][kontur], a B2G company based in Ekaterinburg, Russia. We express gratitude to our company for encouraging us to opensource Moira and for giving back to the community that created [Graphite][graphite] and many other useful DevOps tools.


[web]: https://github.com/moira-alert/web
[cache]: https://github.com/moira-alert/cache
[notifier]: https://github.com/moira-alert/notifier
[readthedocs]: http://moira.readthedocs.org
[gitter]: https://gitter.im/moira-alert/moira
[kontur]: https://kontur.ru/eng/about
[graphite]: http://graphite.readthedocs.org
