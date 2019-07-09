%global debug_package %{nil}
%define pkg_prefix %{?PACKAGE_PREFIX}%{!?PACKAGE_PREFIX:lemur}
%define plugin_dir %{?PLUGIN_DIR}%{!?PLUGIN_DIR:%{_libexecdir}/lhsmd}

Name: %{pkg_prefix}-hsm-agent
Version: %{?_gitver}%{!?_gitver:0.0.1}
Release: %{?dist}%{!?dist:1}

Vendor: Intel Corporation
Source: %{pkg_prefix}-%{version}.tar.gz
Source1: lhsmd.conf
Source2: lhsmd.service
License: GPLv2
Summary: Lustre HSM Tools - Lustre HSM Agent
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires: golang >= 1.6
BuildRequires: pandoc
Requires: lustre-client >= %{?MIN_LUSTRE_VERSION}%{?!MIN_LUSTRE_VERSION:2.6.0}
%{?systemd_requires}

%description
The Lustre HSM Agent provides a backend-agnostic HSM Agent for brokering
communications between a Lustre filesystem's HSM coordinator and
backend-specific data movers.

%package -n %{pkg_prefix}-data-movers
Summary: Lustre HSM Tools - HSM Data Movers
License: Apache
Requires: %{pkg_prefix}-hsm-agent = %{version}

%description -n %{pkg_prefix}-data-movers
These data movers are designed to implement the Lustre HSM Agent's data
movement protocol. When associated with an HSM archive number, a data
mover fulfills data movement requests on behalf of the HSM Agent.

# Links against liblustreapi.
%package -n %{pkg_prefix}-testing
Summary: Lustre HSM Tools - Testing Collateral
License: GPLv2
Requires: %{pkg_prefix}-hsm-agent = %{version} %{pkg_prefix}-data-movers = %{version}

%description -n %{pkg_prefix}-testing
Contains testing collateral for the product. Not intended for production
installations.

# TODO: This has to be GPL because it links against liblustreapi. Can we
# fix that?
%package -n lhsm
Summary: Lustre HSM Tools - Data Movement Control
License: GPLv2
Requires: lustre-client >= %{?MIN_LUSTRE_VERSION}%{?!MIN_LUSTRE_VERSION:2.6.0}

%description -n lhsm
CLI for Lustre data movement control.

%prep

%setup -n %{pkg_prefix}-%{version}
# ohhh myyyy...
cd ..
mkdir -p src/github.com/intel-hpdd
mv %{pkg_prefix}-%{version} src/github.com/intel-hpdd/%{pkg_prefix}
mkdir %{pkg_prefix}-%{version}
mv src %{pkg_prefix}-%{version}

%install
export GOPATH=$PWD:$GOPATH
cd src/github.com/intel-hpdd/%{pkg_prefix}
%{__make} install PREFIX=%{buildroot}/%{_prefix}
%{__make} install-example PREFIX=%{buildroot}/
%{__make} uat-install PREFIX=%{buildroot}/%{_prefix} BUILDROOT=%{buildroot}/

# move datamover plugins to plugin dir
install -d %{buildroot}%{plugin_dir}
for plugin in %{buildroot}/%{_bindir}/lhsm-plugin-*; do
    mv $plugin %{buildroot}/%{plugin_dir}/$(basename $plugin)
done

# move lhsmd to /sbin
install -d %{buildroot}%{_sbindir}
mv %{buildroot}/%{_bindir}/lhsmd %{buildroot}/%{_sbindir}

%if 0%{?el6}
  install -m 700 -d %{buildroot}/%{_localstatedir}/run/lhsmd
  install -d %{buildroot}%{_sysconfdir}/init
  install -p -m 0644 %SOURCE1 %{buildroot}%{_sysconfdir}/init/lhsmd.conf
%endif

%if 0%{?el7}
  install -d %{buildroot}%{_unitdir}
  install -p -m 0644 %SOURCE2 %{buildroot}%{_unitdir}/lhsmd.service
%endif

%post
%if 0%{?el7}
  %systemd_post lhsmd.service
%endif

%preun
%if 0%{?el7}
  %systemd_preun lhsmd.service
%endif

%postun
%if 0%{?el7}
  %systemd_postun_with_restart lhsmd.service
%endif

%files
%defattr(-,root,root)
%{_sbindir}/lhsmd
%{_mandir}/man1/lhsmd.1.gz
%{_sysconfdir}/lhsmd/agent.example
%if 0%{?el6}
  %config %{_sysconfdir}/init/lhsmd.conf
  %{_localstatedir}/run/lhsmd
%endif
%if 0%{?el7}
  %{_unitdir}/lhsmd.service
%endif

%files -n %{pkg_prefix}-data-movers
%defattr(-,root,root)
%{plugin_dir}/lhsm-plugin-posix
%{plugin_dir}/lhsm-plugin-s3
%{plugin_dir}/lhsm-plugin-gcs
%{_mandir}/man1/lhsm-plugin-s3.1.gz
%{_mandir}/man1/lhsm-plugin-posix.1.gz
%{_sysconfdir}/lhsmd/lhsm-plugin-posix.example
%{_sysconfdir}/lhsmd/lhsm-plugin-s3.example
%{_sysconfdir}/lhsmd/lhsm-plugin-gcs.example

%files -n %{pkg_prefix}-testing
%defattr(-,root,root)
%{plugin_dir}/lhsm-plugin-noop
%{_libexecdir}/%{pkg_prefix}-testing/*.race
%{_libexecdir}/%{pkg_prefix}-testing/%{pkg_prefix}-uat-runner
%{_datarootdir}/%{pkg_prefix}/test/features/*.feature

%files -n lhsm
%defattr(-,root,root)
%{_bindir}/lhsm
